import sys
import imp

__all__ = (
        'dyanmicImport',
        )

# ---------------------------------------------------------------------------

def dynamicImport(name, path=None):
    """
    Emulates import to dynamically import modules. This supports importing
    from a specific path or paths.

    derived from python documents

    @param name: module name
    @param path: one or more paths to search for the module

    """

    # short circuit if the module has already been loaded
    try:
        return sys.modules[name]
    except KeyError:
        pass

    components = name.split('.')

    if path is None:
        module = __import__(name)
    else:
        # wrap the path in case it wasn't a list
        if type(path) == type(''):
            path = [path]
            
        f, filename, description = imp.find_module(components[0], path)

        try:
            module = imp.load_module(components[0], f, filename, description)
        finally:
            if f:
                f.close()

        # if we want to get to submodules, use __import__ to load them
        if len(components) > 1:
            module = __import__(name)
        
    ####
        
    for component in components[1:]:
        module = getattr(module, component)
    return module
