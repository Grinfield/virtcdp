import functools
import six
import time

import eventlet
from oslo_utils import timeutils
from oslo_concurrency import processutils


def spawn(func, *args, **kwargs):
    """Passthrough method for eventlet.spawn.

    This utility exists so that it can be stubbed for testing without
    interfering with the service spawns.

    It will also grab the context from the threadlocal store and add it to
    the store on the new thread.  This allows for continuity in logging the
    context when using this method to spawn a new thread.
    """

    @functools.wraps(func)
    def context_wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return eventlet.spawn(context_wrapper, *args, **kwargs)


def spawn_n(func, *args, **kwargs):
    """Passthrough method for eventlet.spawn_n.

    This utility exists so that it can be stubbed for testing without
    interfering with the service spawns.

    It will also grab the context from the threadlocal store and add it to
    the store on the new thread.  This allows for continuity in logging the
    context when using this method to spawn a new thread.
    """
    @functools.wraps(func)
    def context_wrapper(*args, **kwargs):
        func(*args, **kwargs)

    eventlet.spawn_n(context_wrapper, *args, **kwargs)


def convert_version_to_int(version):
    """Convert a version to an integer.

    *version* must be a string with dots or a tuple of integers.

    .. versionadded:: 2.0
    """
    try:
        if isinstance(version, six.string_types):
            version = convert_version_to_tuple(version)
        if isinstance(version, tuple):
            return six.moves.reduce(lambda x, y: (x * 1000) + y, version)
    except Exception as ex:
        msg = "Version %s is invalid." % version
        six.raise_from(ValueError(msg), ex)


def convert_version_to_tuple(version_str):
    """Convert a version string with dots to a tuple.

    .. versionadded:: 2.0
    """
    return tuple(int(part) for part in version_str.split('.'))


def format_cpu_spec(cpuset, allow_ranges=True):
    """Format a libvirt CPU range specification.

    Format a set/list of CPU indexes as a libvirt CPU range
    specification. It allow_ranges is true, it will try to detect
    continuous ranges of CPUs, otherwise it will just list each CPU
    index explicitly.

    :param cpuset: set (or list) of CPU indexes

    :returns: a formatted CPU range string
    """
    # We attempt to detect ranges, but don't bother with
    # trying to do range negations to minimize the overall
    # spec string length
    if allow_ranges:
        ranges = []
        previndex = None
        for cpuindex in sorted(cpuset):
            if previndex is None or previndex != (cpuindex - 1):
                ranges.append([])
            ranges[-1].append(cpuindex)
            previndex = cpuindex

        parts = []
        for entry in ranges:
            if len(entry) == 1:
                parts.append(str(entry[0]))
            else:
                parts.append("%d-%d" % (entry[0], entry[len(entry) - 1]))
        return ",".join(parts)
    else:
        return ",".join(str(id) for id in sorted(cpuset))


def get_pci_address(domain, bus, slot, func):
    return '%s:%s:%s.%s' % (domain, bus, slot, func)


def convert_datetime_to_ts(dt_str):
    if not dt_str:
        return
    dt = timeutils.normalize_time(timeutils.parse_isotime(dt_str))
    if six.PY2:
        time_obj = dt.timetuple()
        ts = time.mktime(time_obj)
    else:
        ts = dt.timestamp()
    return ts


def execute(*cmd, **kwargs):
    return processutils.execute(*cmd, **kwargs)


class UNITS(object):
    """
    Unit constants
    """

    # Binary unit constants.
    Ki = 1024
    "Binary kilo unit"
    Mi = 1024 ** 2
    "Binary mega unit"
    Gi = 1024 ** 3
    "Binary giga unit"
    Ti = 1024 ** 4
    "Binary tera unit"
    Pi = 1024 ** 5
    "Binary peta unit"
    Ei = 1024 ** 6
    "Binary exa unit"
    Zi = 1024 ** 7
    "Binary zetta unit"
    Yi = 1024 ** 8
    "Binary yotta unit"

    # Decimal unit constants.
    k = 1000
    "Decimal kilo unit"
    M = 1000 ** 2
    "Decimal mega unit"
    G = 1000 ** 3
    "Decimal giga unit"
    T = 1000 ** 4
    "Decimal tera unit"
    P = 1000 ** 5
    "Decimal peta unit"
    E = 1000 ** 6
    "Decimal exa unit"
    Z = 1000 ** 7
    "Decimal zetta unit"
    Y = 1000 ** 8
    "Decimal yotta unit"
