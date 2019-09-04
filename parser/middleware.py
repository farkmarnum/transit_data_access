def transform_route(symb):
    transformations = {
        "SS": "SI",
        "GS": "S"
    }
    try:
        return transformations[symb]
    except KeyError:
        return symb

def transform_borough(symb):
    transformations = {
        "M": "Manhattan",
        "Bk": "Brooklyn",
        "Q": "Queens",
        "Bx": "Bronx",
        "SI": "Staten Island",
    }
    try:
        return transformations[symb]
    except KeyError:
        return symb
