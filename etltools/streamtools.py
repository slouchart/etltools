from itertools import starmap, tee as replicate_, filterfalse, zip_longest

import functools
from functools import reduce as reduce_

import operator
from collections import namedtuple

import toolz
from toolz import pipe as pipe_, compose as compose_


def aggregate(aggregator, groupings):
    if aggregator is None:
        aggregator = lambda x: x  # noqa: E731

    for k, g in dict(groupings).items():
        yield k, aggregator(g)


def call_next(iterable):
    it = iter(iterable)

    def inner():
        try:
            return next(it)
        except StopIteration:
            return None

    return inner


def call_next_starred(*items):
    return call_next(iter(items))


def compose(*funcs):
    return compose_(*funcs)


def filtertruefalse(predicate, iterable):
    return filter(predicate, iterable), \
           filterfalse(predicate, iterable)


def groupby(key, iterable):
    return toolz.groupby(key, iterable)


"""
 designing a lookup scheme.
 accepts any Iterable
 for each item in this iterable
 - computes a key value from the item data using a provided callable
 - checks if a provided lookup map contains this key
 if yes:
    - outputs this item optionally enriched with the lookup data using
      another provided callable
  if no:
    if rejects are enabled:
      - outputs this item in a separate iterator
"""


def lookup(iterable, key=None,
           lookup_map=None, enrich=None, enable_rejects=False):

    if key is None:
        key = lambda x: x  # noqa: E731

    if lookup_map is None:
        lookup_map = {}

    if enrich is None:
        map_enrich = lambda x: x  # noqa: E731
    else:
        def map_enrich(e):
            return enrich(e, lookup_map)

    def lookup_(it):
        return map(
            map_enrich,
            filter(
                lambda x: key(x) in lookup_map,
                it
            )
        )

    if enable_rejects:
        src1, src2 = replicate(iterable)
        return lookup_(iter(src1)), \
            filterfalse(
                    lambda x: key(x) in lookup_map,
                    iter(src2)
                )
    else:
        return lookup_(iter(iterable))


def mcompose(*funcs):
    def _composer(f, g):
        def inner(*args, **kwargs):
            return f(*g(*args, **kwargs))
        return inner

    return reduce_(
        _composer,
        funcs
    )


def pipeline(*funcs):
    return compose_(*reversed(funcs))


def pipe(data, *steps):
    return pipe_(data, *steps)


def reduce(function, iterable, initial=None):
    return functools.reduce(
        function, iterable, initial
    )


def replicate(iterable, n=2):  # pragma: no cover
    return replicate_(iterable, n)


def router(predicates, iterable, strict=False):
    if predicates is None or len(predicates) == 0:
        clauses = (
            lambda x: bool(x),
            lambda x: not bool(x)
        )  # works either if strict is True
    else:
        def or_(f1, f2):
            return xargs(
                operator.or_, (f1, f2)
            )

        def and_not_(f1, f2):
            return xargs(
                operator.and_,
                (
                    f1,
                    compose_(
                        operator.not_,
                        f2
                    )
                )
            )

        last_clause = lambda a: False  # noqa: E731
        if strict:
            clauses = list()

            for predicate in predicates:
                last_clause = and_not_(
                    predicate,
                    last_clause
                )
                clauses.append(last_clause)

        else:
            clauses = list(predicates)

        else_ = compose_(
            operator.not_,
            functools.reduce(
                or_,
                predicates,
                last_clause
            )
        )
        clauses = tuple(clauses) + (else_, )

    return tuple(
        starmap(
            filter,
            zip(
                clauses,
                replicate_(
                    iter(iterable),
                    len(clauses)
                )
            )
        )
    )


class stream_converter:

    _dispatcher = dict()

    def __init__(self, from_, to_, *args, **kwargs):

        key_type = kwargs.pop('key_type', None)

        if (from_, to_, key_type) in stream_converter._dispatcher:
            self._method = stream_converter._dispatcher[
                (from_, to_, key_type)
            ](*args, **kwargs)

        else:
            raise TypeError(f"Type association "
                            f"'({from_.__name__}, {to_.__name__})' "
                            f"unsupported by stream_converter")

    def __call__(self, data):
        return self._method(data)

    @classmethod
    def dispatch(cls, t_to, t_from, t_key=None):
        def decorator(func):
            @functools.wraps(func)
            def wrapped(*args, **kwargs):
                return func(*args, **kwargs)

            cls._dispatcher[(t_to, t_from, t_key)] = wrapped
            return wrapped

        return decorator


@stream_converter.dispatch(tuple, tuple)
@stream_converter.dispatch(dict, dict)
@stream_converter.dispatch(namedtuple, namedtuple)
def _identity():
    return toolz.identity


@stream_converter.dispatch(tuple, dict, str)
def _convert_tuple_to_dict(keys):
    if keys and len(keys):
        return lambda d: dict(zip(keys, d))
    else:
        raise ValueError("Converter requires non-empty 'keys' argument")


@stream_converter.dispatch(tuple, dict, int)
def _convert_tuple_to_dict():
    return lambda t: dict(enumerate(t))


@stream_converter.dispatch(tuple, namedtuple)
def _convert_tuple_to_named_tuple(keys, typename='DataStructure'):
    if keys and len(keys):
        return namedtuple(typename, keys)._make
    else:
        raise ValueError("Converter requires non-empty 'keys' argument")


@stream_converter.dispatch(dict, tuple)
def _convert_dict_to_tuple():
    return lambda d: tuple(d.values())


@stream_converter.dispatch(dict, namedtuple)
def _convert_dict_to_namedtuple(typename='DataStructure'):
    return lambda d: namedtuple(typename, d.keys())._make(d.values())


@stream_converter.dispatch(namedtuple, dict)
def _convert_namedtuple_to_dict():
    return lambda nt: dict(nt._asdict())


@stream_converter.dispatch(namedtuple, tuple)
def _convert_namedtuple_to_tuple():
    return lambda nt: tuple(nt)


def stream_generator(keys, funcs, nb_items):

    key_func = {}
    for k, f in zip_longest(keys, funcs, fillvalue=lambda: None):
        if callable(f):
            key_func[k] = f
        else:
            key_func[k] = lambda: f  # noqa: E731

    def make_data():
        return {
            k_: f_() for k_, f_ in key_func.items()
        }

    if nb_items >= 0:
        for _ in range(nb_items):
            yield make_data()
    elif nb_items < 0:
        while True:
            yield make_data()


def xargs(g, funcs, as_iterable=False):
    """returns a function that accepts a tuple as an arguments and then
    maps each element of this tuple to one of the funcs generating another
    tuple in the process. Finally, the function g is called with the tuple
    elements as arguments.
    If the tuple does not contain enough elements to map all the funcs,
    the last element is repeated to provide an argument to the remaining funcs
    """
    def inner(*args):
        evaluated_funcs = tuple(
            starmap(
                lambda f, arg: f(arg),
                zip_longest(funcs, args, fillvalue=args[-1])
            )
        )
        return g(evaluated_funcs) if as_iterable else g(*evaluated_funcs)

    return inner
