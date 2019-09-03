
def transform_symbol(symb):
    transformations = {
        "SS": "SI"
    }
    try:
        return transformations[symb]
    except KeyError:
        return symb
