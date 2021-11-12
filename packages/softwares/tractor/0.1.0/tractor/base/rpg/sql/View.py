"""The View module enables SQL/plpython views to be defined and represented as python objects."""

class View(object):
    def __init__(self, name, definition):
        self.name = name
        self.definition = definition
    def getCreate(self):
        return "CREATE OR REPLACE VIEW %s AS %s;\n" % (self.name, self.definition)

