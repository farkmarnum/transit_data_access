class NestedDict(dict):
    """A dict that automatically creates new dicts within itself as needed"""
    def __getitem__(self, key):
        if key in self:
            return self.get(key)
        return self.setdefault(key, NestedDict())

def trip_to_shape(trip_id):
    """Takes a trip_id in form '092200_6..N03R' and returns what's after the last underscore
    This should be the shape_id ('6..N03R')
    """
    return trip_id.split('_').pop()
