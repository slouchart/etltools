import collections

from itertools import starmap, tee as replicate_, filterfalse, zip_longest
from itertools import chain

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
    src_1, src_2 = replicate(iterable)
    return filter(predicate, src_1), filterfalse(predicate, src_2)


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


class pipable(object):
    def __init__(self, callable_):
        self._callable = callable_
        assert callable(self._callable)

    def __call__(self, *args, **kwargs):
        return self._callable(*args, **kwargs)

    def __or__(self, other):
        return pipable(pipeline(self, other))


def pipeline(*funcs):
    return compose_(*reversed(funcs))


def pipe_data_through(data, *steps):
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


class _splitter:
    """A chimera class, an iterator with the interface of a generator"""
    def __init__(self, callback):
        self._callback = callback
        self._buffer = collections.deque()
        self._should_stop = False
        self._exception = StopIteration

    def send(self, item):
        self._buffer.append(item)

    def throw(self, exc):
        self._should_stop = True
        self._exception = exc

    def __iter__(self):
        return self

    def __call__(self, *args, **kwargs):
        for item in iter(self):
            yield item

    def __next__(self):
        if not self._should_stop:
            if len(self._buffer) == 0:
                self._callback(self)  # draw a new value

            if len(self._buffer) == 0:  # nothing was sent
                self._should_stop = True

            if len(self._buffer) > 0:  # emit the first received element
                return self._buffer.popleft()

        if self._should_stop:
            raise self._exception


class split(object):

    def __new__(cls, func, tuples, expected_length=-1):
        new_instance = super().__new__(cls)
        new_instance.__init__(func, tuples, expected_length=expected_length)
        return new_instance._iterators  # func-like class :wink:

    def __init__(self, func, tuples, expected_length=-1):

        self._splitter_func = func
        self._it = iter(tuples)

        assert callable(self._splitter_func)

        if expected_length > 0:
            nb_splitter = expected_length
        else:
            try:
                first = next(self._it)
                nb_splitter = len(self._splitter_func(first))
                self._it = chain([first], self._it)  # reset the main iterator
            except StopIteration:
                nb_splitter = 0

        if nb_splitter:
            self._iterators = tuple(
                _splitter(self)
                for _ in range(nb_splitter)
            )
        else:
            self._iterators = tuple()

    def __len__(self):  # pragma: no cover
        return len(self._iterators)

    def __call__(self, requester):
        try:
            item = next(self._it)
            try:
                split_item = self._splitter_func(item)
            except Exception:
                raise RuntimeError("Exception in the splitting function")

            for index, t in enumerate(split_item):
                try:
                    self._iterators[index].send(t)
                except IndexError:
                    raise ValueError(
                        "Encountered a tuple with length "
                        "exceeding the number of output "
                        "iterators: " +
                        f"{len(split_item)} > "
                        f"{len(self._iterators)}"
                    )

        except StopIteration:
            requester.throw(StopIteration)
        except (RuntimeError, ValueError) as e:
            self._throw_to_all(e)

    def _throw_to_all(self, exc):
        # signal all iterators
        for it in self._iterators:
            it.throw(exc)


class stream_converter:

    _dispatcher = dict()

    def __init__(self, from_, to_, *args, **kwargs):

        key_type = kwargs.pop('key_type', None)

        if (from_, to_, key_type) in stream_converter._dispatcher:
            meth_factory = stream_converter._dispatcher[
                (from_, to_, key_type)
            ]  # a kind of goofy name isn't it?
            self._method = meth_factory(*args, **kwargs)
            # be insured no meth has been cooked here, only methODs :)
        else:
            raise KeyError(f"Type association "
                           f"'({from_.__name__}, {to_.__name__})' "
                           f"unsupported by stream_converter")

    def __call__(self, data):
        return self._method(data)

    def __or__(self, other):  # chain converters using |
        assert callable(other)
        return pipable(pipeline(self, other))

    @classmethod
    def dispatch(cls, from_, to_, key_type=None):
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            assert (from_, to_, key_type) not in cls._dispatcher, \
                f"A dispatch entry already exists for " \
                f"({from_.__name__}, {to_.__name__}, " \
                f"{key_type.__name__ if key_type is not None else str(key_type)})"  # noqa: E501
            cls._dispatcher[(from_, to_, key_type)] = wrapper
            return wrapper

        return decorator


@stream_converter.dispatch(tuple, tuple)
@stream_converter.dispatch(dict, dict)
@stream_converter.dispatch(namedtuple, namedtuple)
def _identity():
    return toolz.identity


@stream_converter.dispatch(tuple, dict, key_type=str)
def _convert_tuple_to_dict_with_keys(keys):
    if keys and len(keys):
        return lambda d: dict(zip(keys, d))
    else:
        raise ValueError("Converter requires non-empty 'keys' argument")


@stream_converter.dispatch(tuple, dict, key_type=int)
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
