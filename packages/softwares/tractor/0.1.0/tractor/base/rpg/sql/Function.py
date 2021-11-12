"""The Function module enables SQL/plpython functions to be defined and represented as python objects."""

# this preamble is required to load the plpython procedural language extension
FUNCTION_PREAMBLE = r"""
CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;
COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';
CREATE OR REPLACE PROCEDURAL LANGUAGE plpython2u;
ALTER PROCEDURAL LANGUAGE plpython2u OWNER TO root;
"""

class Function(object):
    def __init__(self, name, parameters, returnType, language, body):
        self.name = name
        self.parameters = parameters
        self.returnType = returnType
        self.language = language
        self.body = body

    def getCreate(self):
        return "CREATE OR REPLACE FUNCTION %s (%s) RETURNS %s LANGUAGE %s AS $$\n%s\n$$;"\
               % (self.name, self.parameters, self.returnType, self.language, self.body)


