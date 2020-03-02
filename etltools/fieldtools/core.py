from toolz import keyfilter, itemmap, compose as compose_
from itertools import zip_longest


def fextract(keys, ddict):
    """
    Extracts (key, value) pairs from d if key is in keys
    :param keys: a collection, should support __contains__
    :param ddict: a data dictionary
    :return: a data dictionary
    """
    return keyfilter(
        lambda k: k in keys, ddict
    )


def flookup(lookup_map, keys, ddict):
    result = keyfilter(lambda k: k not in keys, ddict)
    for key in keys:
        if key in ddict:
            if ddict[key] in lookup_map:
                result[key] = lookup_map[ddict[key]]  # found ! :D
            else:
                result[key] = None  # not found :'(
        else:
            pass  # field does not exist :/

    return result


def freverse_lookup(lookup_map, keys, ddict):
    result = keyfilter(lambda k: k not in keys, ddict)
    for key in keys:
        if key in ddict:
            for item in lookup_map:
                if ddict[key] in lookup_map[item]:
                    result[key] = item  # found ! :D
                    break
            else:
                result[key] = None  # not found :'(
        else:
            pass  # field does not exist :/

    return result


def fmap(keys, funcs, ddict, val_as_args=False):
    """

    :param keys: a collection, should support __contains__
                 and __getitem__
    :param funcs: a iterable of callables
    :param ddict: a data dictionary
    :param val_as_args: bool
    :return: a data dictionary
    """
    func_map = dict(
        zip_longest(
            keys, funcs,
            fillvalue=lambda x: x
        )
    )

    def _apply_func(item):
        k, v = item
        if k in func_map:
            if val_as_args:
                return k, func_map[k](*v)
            else:
                return k, func_map[k](v)
        else:
            return k, v

    return dict(
        itemmap(_apply_func, ddict)
    )


def fremove(keys, ddict):
    """
    Remove keys according to a list
    :param keys:
    :param ddict:
    :return:
    """
    return keyfilter(
        lambda k: k not in keys,
        ddict
    )


def frename(keys, ddict):
    """
    Rename keys according to a mapping
    :param keys:
    :param ddict:
    :return:
    """
    def _keys(item):
        k, v = item
        if k in keys:
            return keys[k], v
        else:
            return k, v

    return dict(
        itemmap(_keys, ddict)
    )


def fsplit(keys, ddict):
    return fextract(keys, ddict), fremove(keys, ddict)
