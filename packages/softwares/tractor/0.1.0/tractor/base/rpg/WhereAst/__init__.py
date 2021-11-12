class Error(Exception):
    'Base exception for AST transformation errors.'

    def __init__(self, ast):
        self.ast = ast

class UnknownAstError(Error):
    'Exception when we encounter an unknown ast type.'

    def __str__(self):
        return 'Unknown ast type: %s' % self.ast

# -----------------------------------------------------------------------------

class Ast(object):
    'Abstract base class for the Abstract Syntax Tree'

    def children(self):
        return []

    def clone(self):
        raise NotImplementedError

    def __eq__(self, other):
        raise NotImplementedError

class Value(Ast):
    'An abstract class that represents values.'

class Constant(Value):
    '''
    AST for values that do not change according to the input object. This
    means that these values could be optimized away.
    '''

    def __init__(self, value):
        self.value = value

    def clone(self):
        return self.__class__(self.value)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.value == other.value

class TimeInt(Constant):
    'A constant AST node for an integer that is a unix time_t value.'

class RelativeTimeInt(Value):
    'A dynamic AST node for a seconds relative to the current time.'

    def __init__(self, value):
        self.value = value

    def clone(self):
        return self.__class__(self.value)

class Member(Value):
    'A dynamic AST node for extracting a value from the passed in object.'

    def __init__(self, table, field):
        self.table = table
        self.field = field

    def clone(self):
        return self.__class__(self.table, self.field)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
            self.table == other.table and self.field == other.field

class VirtualMember(Member):
    'A dynamic AST node that is a virtual member in the object.'

class List(Value):
    'An AST that contains a list of ASTs.'

    def __init__(self, values):
        self.values = values

    def children(self):
        return self.values

    def clone(self, *values):
        return self.__class__(values)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
            self.values == other.values

class Expr(Ast):
    'An abstract class that represents expressions.'

class Unary(Expr):
    'An Abstract class that represents unary expressions.'

class Not(Unary):
    'An AST that negates the expression when evaluating.'

    def __init__(self, expr):
        self.expr = expr

    def children(self):
        return [self.expr]

    def clone(self, expr):
        return self.__class__(expr)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.expr == other.expr

class BooleanExpr(Expr):
    'An abstract AST that represents a boolean expression.'

    def __init__(self, exprs):
        self.exprs = exprs

    def children(self):
        return self.exprs

    def clone(self, *exprs):
        return self.__class__(exprs)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.exprs == other.exprs

class And(BooleanExpr):
    '''
    An AST that checks that returns true if all the subexpressions are true.
    '''

class Or(BooleanExpr):
    '''
    An AST that checks that returns true if any the subexpressions are true.
    '''

class Binary(Expr):
    'An abstract base class for ASTs that are a binary operation.'

    def __init__(self, left, right):
        self.left = left
        self.right = right

    def children(self):
        return [self.left, self.right]

    def clone(self, left, right):
        return self.__class__(left, right)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
            self.left == other.left and self.right == other.right

class Eq(Binary):
    '''
    An AST that checks if the left node equals the right node when evaluating.
    '''

class Ne(Binary):
    '''
    An AST that checks if the left node does not equal the right node when
    evaluating.
    '''

class Lt(Binary):
    '''
    An AST that checks if the left node is less than to the right node when
    evaluating.
    '''

class Le(Binary):
    '''
    An AST that checks if the left node is less than or equal to the right
    node when evaluating.
    '''

class Gt(Binary):
    '''
    An AST that checks if the left node is greater than the right node when
    evaluating.
    '''

class Ge(Binary):
    '''
    An AST that checks if the left node is greater than or equal to the
    right node when evaluating.
    '''

class In(Binary):
    '''
    An AST that checks if the left node is contained in the right node when
    evaluating.
    '''

class NotIn(Binary):
    '''
    An AST that checks if the left node is not contained in the right node when
    evaluating.
    '''

class Like(Binary):
    '''
    An AST that checks if the regular expression of the left node matches
    any of the right nodes when evaluating.
    '''

class ConstantLike(Like):
    '''
    An AST that checks if the regular expression of the left node matches any
    of the right nodes when evaluating. The right side of the expression must
    be constant.
    '''

    def regex(self):
        try:
            return self._regex
        except AttributeError:
            self._regex = re.compile(self.right.value)
            return self._regex
    regex = property(regex)

class NotLike(Binary):
    '''
    An AST that checks if the regular expression of the left node does not match
    any of the right nodes when evaluating.
    '''

class ConstantNotLike(NotLike):
    '''
    An AST that checks if the regular expression of the left node does not match
    any of the right nodes when evaluating. The right side of the expression must
    be constant.
    '''

    def regex(self):
        try:
            return self._regex
        except AttributeError:
            self._regex = re.compile(self.left.value)
            return self._regex
    regex = property(regex)

class Has(Binary):
    '''
    An AST that checks if each item in the left node is contained by the right
    nodes when evaluating.
    '''
