"""
determine if a given day is a workday or not.

>>> _set_config_source({})
>>> d = day('1/1/2010')
>>> d.isworkday
True
>>> import datetime
>>> d = day(datetime.date(2010, 1, 2))
>>> d.isworkday
False
"""

import datetime
import yaml

# The day() function and the Day class are the only things we want most clients
# to use; if they want anything else, they'll need to import it manually.
__all__ = ['day', 'Day']

CONFIG_FILE = "/usr/anim/config/rpg/workingsaturdays/workingsaturdays.yaml"

class ConfigError(Exception):
    pass

class Day(object):
    """Represents a day, which may or may not be a workday.

    @ivar day: C{datetime.date} object representing the specified day
    @ivar isworkday: C{True} if the day is a workday; C{False} otherwise.
    @ivar capacity: C{float} between 0.0 and 1.0; provides a hint to clients how
        much usage they should expect on a given day. Unless otherwise
        specified in the config file, workdays have a capacity of 1.0, while
        non-workdays have a capacity of 0.0
    """

    def __init__(self, day, config):
        """Do not use; call L{day()<rpg.workingsaturdays.day>} to instantiate C{Day} objects.

        @type day: L{datetime.date}
        @type config: C{dict}
        """

        self.day = day

        try:
            self.isworkday = config['isworkday']
        except KeyError:
            # if this day is a Saturday or Sunday, default to a holiday
            if day.weekday() > 4:
                self.isworkday = False
            else:
                self.isworkday = True

        if 'capacity' in config:
            try:
                self.capacity = int(config['capacity'].replace('%', '')) / 100.0

                # an explicitly defined capacity should override the default
                # isworkday behavior
                if self.capacity == 0.0:
                    self.isworkday = False
                else:
                    self.isworkday = True
            except ValueError:
                raise ConfigError("unable to interpret capacity '%s'; must be a number" % config.get('capacity'))

            if self.capacity > 1 or self.capacity < 0:
                raise ConfigError("capacity must be between 0 and 100%, inclusive")
        else:
            # default to full capacity on workdays and zero capacity on
            # holidays
            if self.isworkday:
                self.capacity = 1.0
            else:
                self.capacity = 0.0

def str2date(str):
    """Converts a string in the form M/D/Y into a C{datetime.date} object."""
    m, d, y = list(map(int, str.split('/')))
    return datetime.date(y, m, d)

class TimeSpan(object):
    """Represents either a single day or a span of days.

    >>> from datetime import date
    >>> s = TimeSpan('1/1/2010')
    >>> date(2010, 1, 1) in s
    True
    >>> date(2010, 1, 2) in s
    False
    >>> s = TimeSpan('1/1/2010-1/5/2010')
    >>> date(2010, 1, 1) in s
    True
    >>> date(2010, 1, 3) in s
    True
    >>> date(2010, 1, 6) in s
    False
    """

    def __init__(self, spanstr):
        """Converts a date string in the form of either:

            MM/DD/YYYY-MM/DD/YYYY

        or simply:

            MM/DD/YYYY

        into a timespan object.
        """

        try:
            fromstr, tostr = spanstr.split('-')
        except ValueError:
            fromstr = spanstr
            tostr = spanstr

        self.span = list(map(str2date, (fromstr, tostr)))

    def __contains__(self, d):
        """Returns C{True} if C{d} lies within this time range.

        @type d: C{datetime.date}
        """
        return d >= self.span[0] and d <= self.span[1]

    def __repr__(self):
        return "%s('%s/%s/%s-%s/%s/%s')" % (self.__class__.__name__,
                self.span[0].month, self.span[0].day, self.span[0].year,
                self.span[1].month, self.span[1].day, self.span[1].year)

_config_source = CONFIG_FILE
"""Determines where configuration data is read from.

If set to a string, it's interpreted as a path to a YAML file and the
configuration data is read from that file. Otherwise, it is assumed to be a
configuration dictionary.
"""

_config = None

def _get_config_source():
    """Returns the config source.

    If C{_config_source} is a string, returns the parsed YAML at that path;
    otherwise C{_config_source} itself is returned.
    """
    global _config, _config_source

    if isinstance(_config_source, str):
        # the config is going to come from a file, so open 'er up.
        f = open(_config_source)
        try:
            source = yaml.load(f)
        finally:
            f.close()
    else:
        source = _config_source

    return source

def reload_config():
    """Force configuration to be reread on the next call to L{day}."""
    global _config
    _config = None

def parsed_config():
    """Converts the config data into a list of L{TimeSpan}/config-dict tuples.

    The results are cached to avoid unnecessary I/O. Long-running clients
    should call L{reload_config()} periodically to refresh the configuration
    data.
    """
    global _config

    # if we've already parsed the config, just return that
    if _config:
        return _config

    _config = []
    source = _get_config_source()
    sections = (('Working Saturdays', True), ('Holidays', False))
    for section, isworkday in sections:
        for span, span_config in source.get(section, {}).items():
            if span_config is None:
                span_config = {}
            span_config['isworkday'] = isworkday

            span = TimeSpan(span)
            _config.append((span, span_config))

    return _config

def _set_config_source(source):
    """Sets the global configuration source.

    Intended only for testing purposes.
    """
    global _config_source
    _config_source = source

def day(date=None):
    """Returns a L{Day} object for the specified date.

    This is the primary interface for this module.

    @type date: A string in C{M/D/Y} format, or a C{datetime.date} instance.
    """
    if not date:
        date = datetime.date.today()
    elif isinstance(date, str):
        date = str2date(date)
    elif isinstance(date, datetime.datetime):
        date = date.date()

    assert(isinstance(date, datetime.date))

    for span, config in parsed_config():
        if date in span:
            return Day(date, config)

    return Day(date, {})

if __name__ == "__main__":
    import doctest
    doctest.testmod()
