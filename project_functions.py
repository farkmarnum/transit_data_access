import math

_get = getattr
_set = setattr

def _locate_csv(name, gtfs_settings):
    return '%s/%s.txt' % (gtfs_settings.static_data_path, name)

def _rotate(origin, point, angle):
    """
    Rotate a point counterclockwise by a given angle around a given origin.

    The angle should be given in radians.
    """
    ox, oy = origin
    px, py = point

    qx = ox + math.cos(angle) * (px - ox) - math.sin(angle) * (py - oy)
    qy = oy + math.sin(angle) * (px - ox) + math.cos(angle) * (py - oy)
    return qx, qy

class Simp:
    """ Class for simple object that can take attributes

    Usage: o = Simp(attr1 = val1, attr2 = val2, attr3 = val3)
    """
    def __init__(self, **kwds):
        self.__dict__.update(kwds)

    def attr(o):
        #return [a for a in dir(o) if not a.startswith('__') and not a == 'attr']
        return o.__dict__.items()
