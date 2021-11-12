import operator

import rpg.WhereAst as Ast

try:
    all, any
except NameError:
    # Reimplement these for < python 2.5
    def all(values):
        for value in values:
            if not value:
                return False
        return True

    def any(values):
        for value in values:
            if value:
                return True
        return False

# -----------------------------------------------------------------------------

class EvaluationError(Ast.Error):
    'Exception when we fail to evaluate an object with this expression.'

    def __init__(self, ast, msg):
        self.ast = ast
        self.msg = msg

    def __str__(self):
        return self.msg

# -----------------------------------------------------------------------------

def transformAst(ast, transforms):
    '''
    L{transformAst} walks the AST tree, applying each transform in
    depth-first order, and returns the ultimate value from each transform
    on the entire ast.
    '''

    # clone the ast with the transformed values of it's children.
    children = []
    for child in ast.children():
        children.append(transformAst(child, transforms))

    # now, create a new node with the new children
    ast = ast.clone(*children)

    # now apply each transform
    for transform in transforms:
        # transforms may not return a value if it doesn't transform it, so
        # use the ast as the return value.
        ast = transform(ast) or ast

    return ast

# -----------------------------------------------------------------------------

def foldNotAstTransform(ast):
    '''
    L{foldNotAstTransform} evaluates constant not expressions and other simple
    statically inferrable expressions.
    '''

    if not isinstance(ast, Ast.Not):
        return

    # Try to optimize away "not" expressions.

    e = ast.expr
    if isinstance(e, Ast.Not):
        # not not expr => expr
        return e.expr

    elif isinstance(e, Ast.Eq):
        # not left == right => left != right
        return Ast.Ne(e.left, e.right)

    elif isinstance(e, Ast.Ne):
        # not left != right => left == right
        return Ast.Eq(e.left, e.right)

    elif isinstance(e, Ast.Lt):
        # not left < right => left >= right
        return Ast.Ge(e.left, e.right)

    elif isinstance(e, Ast.Le):
        # not left <= right => left > right
        return Ast.Gt(e.left, e.right)

    elif isinstance(e, Ast.Gt):
        # not left > right => left <= right
        return Ast.Le(e.left, e.right)

    elif isinstance(e, Ast.Ge):
        # not left >= right => left < right
        return Ast.Lt(e.left, e.right)

    elif isinstance(e, Ast.In):
        # not left in right => left not in right
        return Ast.NotIn(e.left, e.right)

    elif isinstance(e, Ast.Like):
        # not left like right => left not like right
        return Ast.NotLike(e.left, e.right)

# -----------------------------------------------------------------------------

def foldConstantBooleansTransform(ast):
    'L{foldConstentBooleansTransform} evaluates constant boolean expressions.'

    if not isinstance(ast, Ast.BooleanExpr):
        return

    if len(ast.exprs) == 0:
        # and() => True
        return Ast.Constant(True)

    elif len(ast.exprs) == 1:
        # and(x) => x
        return ast.exprs[0]

    # and(True, False) => False
    for e in ast.exprs:
        if not isinstance(e, Ast.Constant):
            return
    else:
        op = isinstance(ast, Ast.And) and operator.and_ or operator.or_

        value = ast.exprs[0].value
        for e in ast.exprs[1:]:
            value = op(value, e.value)

        return value

# -----------------------------------------------------------------------------

def foldConstantBinaryAstTransform(ast):
    'L{foldConstantBinaryAstTransform} evalutes constant binary expressions.'

    if not isinstance(ast, Ast.Binary):
        return

    # Try to optimize away binary expressions.

    l = ast.left
    r = ast.right
    if isinstance(l, Ast.Constant) and isinstance(r, Ast.Constant):
        # Fold constant binary expressions.

        if isinstance(ast, Ast.Eq):
            # 0 == 1 => False
            return Ast.Constant(l.value == r.value)

        elif isinstance(ast, Ast.Ne):
            # 0 != 1 => True
            return Ast.Constant(l.value != r.value)

        elif isinstance(ast, Ast.Lt):
            # 0 < 1 => True
            return Ast.Constant(l.value < r.value)

        elif isinstance(ast, Ast.Le):
            # 0 < 1 => True
            return Ast.Constant(l.value <= r.value)

        elif isinstance(ast, Ast.Gt):
            # 0 > 1 => False
            return Ast.Constant(l.value > r.value)

        elif isinstance(ast, Ast.Ge):
            # 0 >= 1 => False
            return Ast.Constant(l.value >= r.value)

    elif isinstance(ast, Ast.Like):
        # Convert "like" expressions into constant like expressions

        if isinstance(r, Ast.Constant):
            # convert single value likes
            return Ast.ConstantLike(l, r)

        elif isinstance(r, Ast.List):
            # convert multi-valued likes

            for e in r.values:
                if not isinstance(e, Ast.Constant):
                    break
            else:
                return Ast.ConstantLike(l, r)

# -----------------------------------------------------------------------------

def foldBooleanTransform(ast):
    '''
    L{foldBooleanTransform} flattens a tree of boolean expressions into a list
    of expressions that can be either "all"ed or "any"ed.
    '''

    if not isinstance(ast, Ast.BooleanExpr):
        return

    # (a and b) and (c and d) => (and a b c d)
    # (and a (and b c) (or d e)) => (and a b c (or d e))
    exprs = []
    for e in ast.exprs:
        if isinstance(e, ast.__class__):
            exprs.extend(e.exprs)
        else:
            exprs.append(e)

    return ast.__class__(exprs)

# -----------------------------------------------------------------------------

def liftOrAstToInAstTransform(ast):
    '''
    L{liftOrAstToInAstTransform} converts a list of constant binary expression
    into an L{Ast.In} (for L{Ast.Eq}) or L{Ast.NotIn} (for L{Ast.Ne}) if they
    all share the same left member.
    '''

    if not isinstance(ast, Ast.Or):
        return

    # or(a=b, a=c, a=d, x=y, x=z) => a in [b c d] or x in [y z]

    inAsts = []
    notInAsts = []
    otherAsts = []

    # classify the expressions as either being suitable for Ast.Ins,
    # Ast.NotIns, or everything else.
    for e in ast.exprs:
        if isinstance(e, (Ast.Eq, Ast.In)):
            asts = inAsts
            Class = Ast.In
        elif isinstance(e, (Ast.Ne, Ast.NotIn)):
            asts = notInAsts
            Class = Ast.NotIn
        else:
            otherAsts.append(e)
            continue

        if isinstance(e.right, Ast.List):
            values = e.right.values
        else:
            values = [e.right]

        # Check each expression to see if it's left member matches the ast
        # member. If so, add that expression to the Ast.In.
        for a in asts:
            if e.left == a.left:
                a.right.values.extend(values)
                break
        else:
            asts.append(Class(e.left, Ast.List(values)))

    # now, add each Ast.In to the expression
    exprs = []
    for inAst in inAsts:
        # if we only have one value, just use an Ast.Eq
        if len(inAst.right.values) == 1:
            exprs.append(Ast.Eq(inAst.left, inAst.right.values[0]))
        else:
            exprs.append(inAst)

    for notInAst in notInAsts:
        # if we only have one value, just use an Ast.Ne
        if len(notInAst.right.values) == 1:
            exprs.append(Ast.Ne(notInAst.left, notInAst.right.values[0]))
        else:
            # We need to run the ast through the pass manager since it may
            # have new values.
            exprs.append(notInAst)

    exprs.extend(otherAsts)

    # If we result in just one expression, return that, otherwise OR all the
    # values together.
    if len(exprs) == 1:
        return exprs[0]
    else:
        return Ast.Or(exprs)

# -----------------------------------------------------------------------------

def deadCodeEliminationTransform(ast):
    '''
    L{deadCodeEliminationTransform} removes duplicate code and prunes
    expressions that will never be reached.
    '''

    if isinstance(ast, Ast.BooleanExpr):
        # and(a, b, c, d, a, b) => and(a, b, c, d)

        exprs = []
        for e in ast.exprs:
            if e not in exprs:
                exprs.append(e)

        return ast.clone(*exprs)

    elif isinstance(ast, (Ast.In, Ast.NotIn, Ast.Like, Ast.NotLike)) and \
            isinstance(ast.right, Ast.List):
        # a in [1 2 3 2 1] => a in [1 2 3]
        # a like [1 2 3 2 1] => a like [1 2 3]

        values = []

        for value in ast.right.values:
            if value not in values:
                values.append(value)

        return ast.clone(ast.left, Ast.List(values))

# -----------------------------------------------------------------------------

def expandAst(ast):
    if isinstance(ast, Ast.NotIn):
        return Ast.Not(Ast.In(ast.left, ast.right))

    elif isinstance(ast, Ast.NotLike):
        return Ast.Not(Ast.Like(ast.left, ast.right))

# -----------------------------------------------------------------------------

def optimizeAst(ast, extraTransforms=[]):
    'L{optimizeAst} walks the AST tree and does basic optimizations.'

    return transformAst(ast, [
        foldNotAstTransform,
        foldConstantBooleansTransform,
        foldConstantBinaryAstTransform,
        foldBooleanTransform,
        deadCodeEliminationTransform,
        liftOrAstToInAstTransform,
    ] + extraTransforms)

# -----------------------------------------------------------------------------

def evaluateAst(ast, obj):
    'Evaluate the AST tree.'

    if isinstance(ast, Ast.Constant):
        return ast.value

    elif isinstance(ast, Ast.RelativeTimeInt):
        return int(time.time()) + ast.value

    elif isinstance(ast, Ast.Member):
        # look up the member in the object
        if ast.table == ast.field.table:
            return getattr(obj, ast.field.member)
        return getattr(getattr(obj, ast.field.classObject.__name__),
                ast.field.member)

    elif isinstance(ast, Ast.List):
        return [evaluateAst(value, obj) for v in ast.values]

    elif isinstance(ast, Ast.Not):
        return not evaluateAst(ast.expr, obj)

    elif isinstance(ast, Ast.And):
        return all([evaluateAst(e, obj) for e in ast.exprs])

    elif isinstance(ast, Ast.Eq):
        return evaluateAst(ast.left, obj) == evaluateAst(ast.right, obj)

    elif isinstance(ast, Ast.Ne):
        return evaluateAst(ast.left, obj) != evaluateAst(ast.right, obj)

    elif isinstance(ast, Ast.Lt):
        return evaluateAst(ast.left, obj) < evaluateAst(ast.right, obj)

    elif isinstance(ast, Ast.Le):
        return evaluateAst(ast.left, obj) <= evaluateAst(ast.right, obj)

    elif isinstance(ast, Ast.Gt):
        return evaluateAst(ast.left, obj) > evaluateAst(ast.right, obj)

    elif isinstance(ast, Ast.Ge):
        return evaluateAst(ast.left, obj) >= evaluateAst(ast.right, obj)

    elif isinstance(ast, Ast.In):
        return evaluateAst(ast.left, obj) in evaluateAst(ast.right, obj)

    elif isinstance(ast, Ast.NotIn):
        return evaluateAst(ast.left, obj) not in evaluateAst(ast.right, obj)

    elif isinstance(ast, (Ast.Like, Ast.NotLike)):
        # check if we can use the compiled regex
        try:
            regexes = ast.regex
        except AttributeError:
            # compile each regex
            right = evaluateAst(ast.right, obj)
            if isinstance(right, list):
                regexes = [re.compile(s) for s in right]
            else:
                regexes = [re.compile(right)]

        expr = evaluateAst(ast.left, obj)

        if not isinstance(expr, list):
            expr = [expr]

        if isinstance(ast, Ast.Like):
            # Ast.Like are OR's of all the values, so return True if we find
            # any matching pattern.
            for e in expr:
                for regex in regexes:
                    if regex.search(e or '') is not None:
                        return True

            return False
        else:
            # Ast.NotLike are NOT AND's of all the values, so return False if
            # we find any non-matching pattern.
            for e in expr:
                for regex in regexes:
                    if regex.search(e or '') is None:
                        return False

            return True

    elif isinstance(ast, Ast.Has):
        if not isinstance(ast.left, Ast.List):
            raise EvaluationError(ast,
                'left node in Ast.Has is not a Ast.List')

        if not isinstance(ast.right, Ast.List):
            raise EvaluationError(ast,
                'right node in Ast.Has is not a Ast.List')

        left = evaluateAst(ast.left, obj)
        right = evaluateAst(ast.right, obj)

        for item in right:
            if item not in left:
                return False
        return True

    else:
        raise EvaluationError(ast, 'unknown ast type: %s' % type(ast))

# -----------------------------------------------------------------------------

def convertAstToWhere(ast):
    """
    L{convertAstToWhere} walks the AST tree and converts it to a where string.
    """

    if isinstance(ast, Ast.Constant):
        return str(ast.value)

    elif isinstance(ast, Ast.Member):
        if ast.table == ast.field.table:
            return ast.field.fieldname

        return '%s.%s' % (ast.table.tablename, ast.field.fieldname)

    elif isinstance(ast, Ast.List):
        return '[%s]' % ' '.join([convertAstToWhere(v) for v in ast.values])

    elif isinstance(ast, Ast.Not):
        return 'not %s' % convertAstToWhere(ast.expr)

    elif isinstance(ast, Ast.BooleanExpr):
        ss = []
        for e in ast.exprs:
            s = convertAstToWhere(e)
            if isinstance(e, type(ast)):
                s = '(' + s + ')'
            ss.append(s)

        op = {Ast.And: ' and ', Ast.Or: ' or '}[type(ast)]
        return op.join(ss)

    elif isinstance(ast, Ast.Binary):
        l = convertAstToWhere(ast.left)
        r = convertAstToWhere(ast.right)

        if isinstance(ast, Ast.Eq):
            op = '='
        elif isinstance(ast, Ast.Ne):
            op = '!='
        elif isinstance(ast, Ast.Le):
            op = '<'
        elif isinstance(ast, Ast.Lt):
            op = '<='
        elif isinstance(ast, Ast.Gt):
            op = '>'
        elif isinstance(ast, Ast.Ge):
            op = '>='
        elif isinstance(ast, Ast.In):
            op = ' in '
        elif isinstance(ast, Ast.NotIn):
            op = ' not in '
        elif isinstance(ast, Ast.Like):
            op = ' like '
        elif isinstance(ast, Ast.NotLike):
            op = ' not like '
        elif isinstance(ast, Ast.Has):
            op = ' has '

        return '%s%s%s' % (l, op, r)

    else:
        raise Ast.UnknownAstError(ast)

# -----------------------------------------------------------------------------

def convertAstToSexp(ast):
    """
    L{convertAstToSexp} walks the AST tree and converts it into a s-expression
    list, where the first item is the name of the node, and the rest are the
    values of that node. This is primary used as a debugging tool in
    combination with pprint.pprint.
    """

    sexp = [ast.__class__.__name__]

    if isinstance(ast, Ast.Constant):
        sexp.append(ast.value)

    elif isinstance(ast, Ast.Member):
        sexp.append(ast.table.tablename)
        sexp.append(ast.field.fieldname)

    elif isinstance(ast, Ast.List):
        sexp.append([convertAstToSexp(v) for v in ast.values])

    elif isinstance(ast, Ast.Not):
        sexp.append(convertAstToSexp(ast.expr))

    elif isinstance(ast, Ast.BooleanExpr):
        sexp.append([convertAstToSexp(e) for e in ast.exprs])

    elif isinstance(ast, Ast.Binary):
        sexp.append(convertAstToSexp(ast.left))
        sexp.append(convertAstToSexp(ast.right))

    return sexp
