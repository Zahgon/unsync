import asyncio
import concurrent
import functools
import inspect
import threading
from threading import Thread
from typing import Generic, TypeVar

class unsync_meta(type):

    def _init_loop(cls):
        pass

    @property
    def loop(cls):
        pass

    @property
    def thread(cls):
        pass

    @property
    def process_executor(cls):
        pass


class unsync(object, metaclass=unsync_meta):
    thread_executor = concurrent.futures.ThreadPoolExecutor()
    process_executor = None
    unsync_functions = {}

    @staticmethod
    def _thread_target(loop):
        pass

    def __init__(self, *args, **kwargs):
        self.args = []
        self.kwargs = {}
        if len(args) == 1 and _isfunction(args[0]):
            self._set_func(args[0])
        else:
            self.args = args
            self.kwargs = kwargs
            self.func = None

    @property
    def cpu_bound(self):
        pass

    def _set_func(self, func):
        pass

    def __call__(self, *args, **kwargs):
        if self.func is None:
            self._set_func(args[0])
            return self
        if inspect.iscoroutinefunction(self.func):
            if self.cpu_bound:
                raise TypeError('The CPU bound unsync function %s may not be async or a coroutine' % self.func.__name__)
            future = self.func(*args, **kwargs)
        else:
            if self.cpu_bound:
                future = unsync.process_executor.submit(
                    _multiprocess_target, (self.func.__module__, self.func.__name__), *args, **kwargs)
            else:
                future = unsync.thread_executor.submit(self.func, *args, **kwargs)
        return Unfuture(future)

    def __get__(self, instance, owner):
        def _call(*args, **kwargs):
            pass

        functools.update_wrapper(_call, self.func)
        return _call


def _isfunction(obj):
    pass


def _multiprocess_target(func_name, *args, **kwargs):
    pass


T = TypeVar('T')


class Unfuture(Generic[T]):
    @staticmethod
    def from_value(value):
        pass

    def __init__(self, future=None):
        def callback(source, target):
            pass

        if asyncio.iscoroutine(future):
            future = asyncio.ensure_future(future, loop=unsync.loop)
        if isinstance(future, concurrent.futures.Future):
            self.concurrent_future = future
            self.future = asyncio.Future(loop=unsync.loop)
            self.future._loop.call_soon_threadsafe(callback, self.concurrent_future, self.future)
        else:
            self.future = future or asyncio.Future(loop=unsync.loop)
            self.concurrent_future = concurrent.futures.Future()
            self.future._loop.call_soon_threadsafe(callback, self.future, self.concurrent_future)

    def __iter__(self):
        return self.future.__iter__()

    __await__ = __iter__

    def result(self, *args, **kwargs) -> T:
        # The asyncio Future may have completed before the concurrent one
        pass

    def done(self):
        pass

    def set_result(self, value):
        pass

    @unsync
    async def then(self, continuation):
        pass
