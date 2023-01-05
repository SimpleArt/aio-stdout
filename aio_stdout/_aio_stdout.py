'''
The purpose of this package is to provide asynchronous variants of
the builtin `input` and `print` functions. `print` is known to be
relatively slow compared to other operations. `input` is even slower
because it has to wait for user input. While these slow IO
operations are being ran, code using `asyncio` should be able to
continuously run.

ainput and aprint
------------------

With `aio_stdout`, the `aio_stdout.ainput` and `aio_stdout.aprint`
functions provide easy to use functionality with organized behaviour.

```python
import asyncio
from aio_stdout import ainput, aprint

async def countdown(n: int) -> None:
    """Count down from `n`, taking `n` seconds to run."""
    for i in range(n, 0, -1):
        await aprint(i)
        await asyncio.sleep(1)

async def get_name() -> str:
    """Ask the user for their name."""
    name = await ainput("What is your name? ")
    await aprint(f"Your name is {name}.")
    return name

async def main() -> None:
    await asyncio.gather(countdown(15), get_name())

if __name__ == "__main__":
    asyncio.run(main())
```

Example output:

```
15
What is your name? Jane
14
13
12
11
10
9
8
Your name is Jane.
7
6
5
4
3
2
1
```

Notice that while the prompt `"What is your name? "` is being waited
for, the `countdown` continues to `aprint` in the background, without
becoming blocked. The `countdown` does not, however, display its
results until the `ainput` is completed. Instead it waits for the
`ainput` to finish before flushing out all of the queued messages.

It is worth noting that with naive threading, a normal attempt to use
`print` while waiting on an `input` leads to overlapping messages.
Fixing this behavior requires a lot more work than should be needed
to use a simple `print` or `input` function, which is why this
package exists. To remedy this problem, queues are used to store
messages until they are ready to be printed. 

IO Locks
---------

Although the asynchronization behaviors of `ainput` and `aprint` are
nice, sometimes we want to be able to synchronize our messages even
more. IO locks provide a way to group messages together, locking the
global `aio_stdout` queues until it finishes or yields access.

```python
import asyncio
from aio_stdout import IOLock, ainput, aprint

async def countdown(n: int) -> None:
    """Count down from `n`, taking `n` seconds to run."""
    async with IOLock(n=5) as lock:
        for i in range(n, 0, -1):
            await lock.aprint(i)
            await asyncio.sleep(1)

async def get_name() -> str:
    """Ask the user for their name."""
    async with IOLock() as lock:
        name = await lock.ainput("What is your name? ")
        await lock.aprint(f"Your name is {name}.")
    return name

async def main() -> None:
    await asyncio.gather(countdown(15), get_name())

if __name__ == "__main__":
    asyncio.run(main())
```

Let's try the example again now using the new locks:

```
15
14
13
12
11
What is your name? Jane
Your name is Jane.
10
9
8
7
6
5
4
3
2
1
```

Notice that this time the `countdown` does not immediately yield to
the `get_name`. Instead, it runs 5 messages before yielding control
over to `get_name`. Now, after the `ainput` finishes, it does not
yield to `countdown`. Instead, it runs its own `aprint` first. In the
meantime, `countdown` continues to run in the background and flushes
all of its buffered messages afterwards.

Flushing
---------

Since messages may be delayed, it is possible for your asynchronous
code to finish running before all messages are displayed, producing
confusing results. As such, the best recommended practice is to flush
from `main` before terminating.

```python
from aio_stdout import flush

@flush
async def main() -> None:
    ...
```

Final Example
-------------

Combining all best practices, the final example should look something
like this:

```python
import asyncio
from aio_stdout import IOLock, ainput, aprint, flush

async def countdown(n: int) -> None:
    """Count down from `n`, taking `n` seconds to run."""
    for i in range(n, 0, -1):
        await aprint(i)
        await asyncio.sleep(1)

async def get_name() -> str:
    """Ask the user for their name."""
    async with IOLock() as lock:
        name = await lock.ainput("What is your name? ")
        await lock.aprint(f"Your name is {name}.")
    return name

@flush
async def main() -> None:
    await asyncio.gather(countdown(15), get_name())

if __name__ == "__main__":
    asyncio.run(main())
```

Common Gotchas
---------------

- Using `input` or `print` instead of `ainput` and `aprint` will push
  a message immediately to the console, potentially conflicting with
  `ainput` or `aprint`.
- Using `ainput` or `aprint` instead of `lock.ainput` and
  `lock.aprint` may produce **deadlock** due to having to
  wait for the lock to release. As such, the `lock` is equipped
  with a default `timeout` limit of 10 seconds to avoid deadlock
  and explain to users this potential problem.
'''
import asyncio
import functools
import logging
import sys
import typing

from asyncio import Queue
from enum import Enum
from functools import partial
from types import TracebackType
from typing import Any, ClassVar, Generic, IO, NoReturn, Optional, TypeVar, Union, get_type_hints, overload

if sys.version_info < (3, 9):
    from typing import Dict, Tuple, Type
else:
    from builtins import dict as Dict, tuple as Tuple, type as Type

if sys.version_info < (3, 11):
    from typing import NoReturn as Never
    Self = TypeVar("Self", bound="IOLock")
else:
    from typing import Never, Self

if sys.version_info < (3, 8):
    from builtins import dict as PrintKwargs
else:
    class PrintKwargs(typing.TypedDict, total=False):
        sep: Optional[str]
        end: Optional[str]
        file: IO
        flush: Any

# Make `asyncio.Queue` generic for type-hinting.
if sys.version_info < (3, 9):
    from asyncio import Queue as IOQueueType
else:
    IOQueueType = Queue[Tuple[bool, Optional[asyncio.Event], Tuple[str, ...], PrintKwargs]]

__all__ = ["IOLock", "ainput", "aprint", "flush"]

T = TypeVar("T")
ET = TypeVar("ET", bound=BaseException)

if sys.version_info < (3, 10):
    FT = TypeVar("FT", bound=typing.Callable[..., typing.Awaitable[Any]])

    class AsyncContextDecorator:

        def __call__(self: "AsyncContextDecorator", func: FT) -> FT:
            @functools.wraps(func)
            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                async with self:
                    return await func(*args, **kwargs)
            return wrapper

        def __aenter__(self: "AsyncContextDecorator") -> Any:
            raise NotImplementedError

        def __aexit__(
            self: "AsyncContextDecorator",
            exc_type: Optional[Type[ET]],
            exc_value: Optional[ET],
            traceback: Optional[TracebackType],
        ) -> Any:
            raise NotImplementedError

else:
    from contextlib import AsyncContextDecorator

logger = logging.getLogger(__name__)


class IOLock(asyncio.Lock):
    """
    The `IOLock` may be used to control the order with which `ainput` and
    `aprint` are scheduled.

    The `IOLock` blocks new IO requests from directly entering the `IO_QUEUE`
    by moving them to the `UNLOCKED_QUEUE` instead.

    Use `IOLock.ainput` and `IOLock.aprint` to within its context block to
    schedule locked IO requests.

    Attributes
    -----------
    Construct an IOLock using:
        >>> lock = IOLock(n=..., timeout=...)
    By default, `n = None` and `timeout = 10`.

    n:
        The number of io requests that can be queued at a time
        before letting other io requests go through.
    timeout:
        The number of seconds the io lock can sleep before letting other
        io requests go through.

    See `help(IOLock.n)` or `help(IOLock.timeout)` for more information.

    Example
    --------
    Use it as a context manager to ensure you can't have printed messages
    in-between them.
        >>> async with IOLock() as lock:
        ...     name = await lock.ainput("What is your name? ")
        ...     await lock.aprint(f"Your name is {name}.")
        ...
        What is your name? (...)
        Your name is (...).

    WARNING
    --------
    Using `aprint` with `block=True` or `ainput` inside of an `lock`
    block will cause deadlock, preventing your program from continuing.
    Use `lock.ainput` and `lock.aprint` instead.

    Using `aprint` with `block=False` inside of an `lock` block will
    delay the `aprint` until the `lock` block is finished.

    With the default `lock.timeout` however, such deadlocks only hold for 10 seconds.
    """
    _class_is_finished: ClassVar[asyncio.Event] = asyncio.Event()
    if sys.version_info < (3, 9):
        _class_queue: ClassVar[Queue] = Queue()
    else:
        _class_queue: ClassVar[Queue[Tuple[Optional[float], IOQueueType, asyncio.Event, asyncio.Event]]] = Queue()
    _i: int
    _is_awake: asyncio.Event
    _is_finished: asyncio.Event
    _n: Optional[int]
    _queue: IOQueueType
    _timeout: Optional[float]

    __slots__ = ("_i", "_is_awake", "_is_finished", "_n", "_queue", "_timeout")

    # Finished running IO because there's nothing being ran yet.
    _class_is_finished.set()

    def __init__(self: Self, *args: Any, n: Optional[int] = None, timeout: Optional[float] = 10, **kwargs: Any) -> None:
        if n is not None and not isinstance(n, int):
            raise TypeError(f"n must be an integer or None, got {x!r}")
        elif timeout is not None and not isinstance(timeout, (int, float)):
            raise TypeError(f"timeout must be an positive number or None, got {timeout!r}")
        elif n is not None and not n > 0:
            raise ValueError(f"n must be greater than 0, got {n!r}")
        elif timeout is not None and not timeout > 0:
            raise ValueError(f"timeout must be greater than 0, got {timeout!r}")
        super().__init__(*args, **kwargs)
        self._i = 0
        self._is_awake = asyncio.Event()
        self._is_finished = asyncio.Event()
        self._n = n
        self._queue = Queue()
        self._timeout = float(timeout) if isinstance(timeout, int) else timeout
        # The lock is not sleeping because it's not being executed.
        self._is_awake.set()
        # Finished running IO because there's nothing being ran yet.
        self._is_finished.set()

    async def __aenter__(self: Self) -> Self:
        """Acquire the lock and return itself."""
        await super().__aenter__()
        return self

    @overload
    async def acquire(self: Self, _: Never) -> NoReturn:
        ...

    if sys.version_info < (3, 8):
        @overload
        async def acquire(self: Self) -> bool:
            ...

    else:
        @overload
        async def acquire(self: Self) -> typing.Literal[True]:
            ...

    async def acquire(self):
        """
        Acquire a lock.

        This method blocks until the lock is unlocked, then sets it to
        locked and returns True.

        This prevents other `ainput` or `aprint` from running.
        """
        await super().acquire()
        # Once the lock is acquired, add it to the queue.
        self._is_finished.clear()
        await type(self)._class_queue.put((self.timeout, self._queue, self._is_awake, self._is_finished))
        # Restart the class executor if necessary.
        if type(self)._class_is_finished.is_set():
            type(self)._class_is_finished.clear()
            asyncio.create_task(type(self)._execute_io())
            # The lock is sleeping because there's nothing being ran yet.
            self._is_awake.clear()

    def release(self: Self) -> None:
        """
        Release a lock.

        When the lock is locked, reset it to unlocked, and return.
        If any other coroutines are blocked waiting for the lock to become
        unlocked, allow exactly one of them to proceed.

        When invoked on an unlocked lock, a RuntimeError is raised.

        There is no return value.
        """
        super().release()
        self._is_finished.set()
        # Use a new `is_awake` event.
        self._is_awake = asyncio.Event()
        self._is_awake.set()
        # Use a new `is_finished` event.
        self._is_finished = asyncio.Event()
        self._is_finished.set()
        # Collect future IO in an empty queue.
        if not self._queue.empty():
            self._queue = Queue()

    @classmethod
    async def __exhaust_queue(cls: Type[Self], io_queue: IOQueueType) -> None:
        """Helper method to exhaust a queue."""
        # Otherwise the io lock is not sleeping and the io queue should be exhausted.
        while not io_queue.empty():
            # Get the next io request.
            is_print, event, args, kwargs = await io_queue.get()
            # Execute the io request in `asyncio`'s default thread.
            if is_print:
                try:
                    await asyncio.get_running_loop().run_in_executor(None, partial(print, *args, **kwargs))
                except Exception as e:
                    if event is None:
                        logger.exception(e)
                    else:
                        PRINT_EXCEPTIONS[event] = e
            else:
                try:
                    INPUT_RESULTS[event] = (False, await asyncio.get_running_loop().run_in_executor(None, partial(input, *args)))
                except Exception as e:
                    INPUT_RESULTS[event] = (True, e)
            # Signal the io request was completed.
            if event is not None:
                event.set()
            io_queue.task_done()

    @classmethod
    async def __wait_event(cls: Type[Self], event: asyncio.Event, message: str) -> str:
        """Helper method to wait until an event occurs."""
        await event.wait()
        return message

    @classmethod
    async def _execute_io(cls: Type[Self]) -> None:
        """Helper method for executing IO requests."""
        while not cls._class_queue.empty():
            timeout, io_queue, is_awake, is_finished = await cls._class_queue.get()
            is_finished_task = asyncio.create_task(cls.__wait_event(is_finished, "finished"))
            task_type = "awake"
            # Wait for the queue to be finished.
            while task_type == "awake":
                # Otherwise the io lock is awake and the io queue should be exhausted.
                await cls.__exhaust_queue(io_queue)
                # Sleep once all tasks are done.
                is_awake.clear()
                # Wait until the queue is either finished or awake.
                tasks = [
                    is_finished_task,
                    asyncio.create_task(cls.__wait_event(is_awake, "awake")),
                ]
                if timeout is None:
                    as_completed = asyncio.as_completed(tasks)
                else:
                    as_completed = asyncio.as_completed(tasks, timeout=timeout)
                for task in as_completed:
                    try:
                        task_type = await task
                    except asyncio.TimeoutError:
                        task_type = "timeout"
                    break
                # Stop checking if it is awake or not.
                tasks[1].cancel()
                try:
                    await tasks[1]
                except asyncio.CancelledError:
                    pass
            cls._class_queue.task_done()
            # Wake up if finished.
            if task_type == "finished":
                is_awake.set()
                # Finish the remaining io requests.
                await cls.__exhaust_queue(io_queue)
            # Otherwise it timed out and needs to be re-added it to the queue.
            else:
                # Warn the user if they timed out after 10 seconds and other IO is waiting.
                if None is not timeout >= 10 and not (cls._class_queue.empty() and IO_QUEUE.empty()):
                    print(
                        "An `IOLock` timed out after 10 seconds or more.",
                        "This is likely due to the use of `aprint` or `ainput`",
                        "instead of `lock.aprint` or `lock.ainput` while",
                        "inside of a `async with lock` block."
                    )
                # Insert the global queue into the class queue.
                global_queue = Queue()
                for _ in range(IO_QUEUE.qsize()):
                    global_queue.put_nowait(IO_QUEUE.get_nowait())
                global_is_finished = asyncio.Event()
                global_is_finished.set()
                await cls._class_queue.put((None, global_queue, asyncio.Event(), global_is_finished))
                await cls._class_queue.put((timeout, io_queue, is_awake, is_finished))
        # Signal no io lock is executing.
        cls._class_is_finished.set()
        # Restart the global executor if necessary.
        if IS_FINISHED.is_set():
            IS_FINISHED.clear()
            asyncio.create_task(_execute_io())

    def _schedule_io(self: Self, is_print: bool, event: Optional[asyncio.Event], args: Tuple[str, ...], kwargs: Optional[PrintKwargs]) -> None:
        """Helper method for scheduling IO requests."""
        # Insert the next IO request.
        self._queue.put_nowait((is_print, event, args, kwargs))
        # Update the lock counter.
        self._i += 1
        # Refresh the lock if necessary.
        if None is not self.n <= self._i:
            self._i = 0
            # The current queue is finished.
            self._is_finished.set()
            # Use a new `is_awake` event.
            self._is_awake = asyncio.Event()
            self._is_awake.set()
            # Use a new `is_finished` event.
            self._is_finished = asyncio.Event()
            # Use a new `queue`.
            self._queue = Queue()
            # Re-add it to the class queue.
            type(self)._class_queue.put_nowait((self.timeout, self._queue, self._is_awake, self._is_finished))
        # The io lock is no longer sleeping, if it was.
        else:
            self._is_awake.set()

    async def ainput(self: Self, *args: Any) -> str:
        """Locked version of `ainput`. See `ainput` for more details."""
        # Perform early type-checking on args.
        if len(args) > 1:
            raise TypeError(f"ainput expected at most 1 argument, got {len(args)}")
        # Require the io lock to be locked.
        elif not self.locked():
            raise RuntimeError(f"ainput used before the lock was acquired")
        # Wait for the io to finish.
        is_completed = asyncio.Event()
        # Schedule the `input`.
        self._schedule_io(False, is_completed, (*[str(arg) for arg in args],), {})
        # Wait for the `input` to finish.
        await is_completed.wait()
        # Collect the result.
        had_exception, response = INPUT_RESULTS.pop(is_completed)
        if had_exception:
            raise response
        else:
            return response

    async def aprint(self: Self, *args: Any, block: bool = False, **kwargs: Any) -> None:
        """Locked version of `aprint`. See `aprint` for more details."""
        # Perform early type-checking on kwargs.
        for kwarg, value in kwargs.items():
            if kwarg in ("sep", "end") and value is not None and not isinstance(value, str):
                raise TypeError(f"{kwarg} must be None or a string, not {type(value).__name__}")
            elif kwarg == "file" and not isinstance(value, IO):
                raise TypeError(f"file must be an IO instance, not {type(value).__name__}")
            elif kwarg not in ("sep", "end", "file", "flush"):
                raise TypeError(f"{kwarg!r} is an invalid keyword argument for aprint()")
        # Require the io lock to be locked.
        if not self.locked():
            raise RuntimeError(f"ainput used before the lock was acquired")
        # Wait for the io to finish depending on `block`.
        event = asyncio.Event() if block else None
        # Schedule the `print`.
        self._schedule_io(True, event, (*[str(arg) for arg in args],), kwargs)
        # Wait for the `print` to finish.
        if block:
            await event.wait()
        # Wait at least once before returning so that the print can start running.
        else:
            await asyncio.sleep(0)

    @property
    def n(self: Self) -> Optional[int]:
        """
        The number of io requests that can be queued at a time
        before letting other io requests go through.

        If `None`, then it blocks until all locked io requests go through.
        """
        return self._n

    @property
    def timeout(self: Self) -> Optional[float]:
        """
        The number of seconds the io lock can sleep before letting other
        io requests go through.

        If `None`, then it blocks until all locked io requests go through.
        """
        return self._timeout


class Flush(AsyncContextDecorator, Enum):
    """
    Helper class to block until all io is flushed.

    Decorator Usage:
        @flush
        async def main() -> None:
            ...

    Context Manager Usage:
        async def main() -> None:
            async with flush:
                ...
    """
    flush = ()

    async def __aenter__(self: "Flush") -> None:
        pass

    async def __aexit__(
        self: "Flush",
        exc_type: Optional[Type[ET]],
        exc_value: Optional[ET],
        traceback: Optional[TracebackType],
    ) -> None:
        """Waits until all IO is flushed."""
        await IOLock._class_is_finished.wait()
        await IS_FINISHED.wait()


flush: Flush = Flush.flush

if sys.version_info < (3, 8):
    INPUT_RESULTS: Dict[asyncio.Event, Tuple[bool, Union[Exception, str]]] = {}
else:
    INPUT_RESULTS: Dict[asyncio.Event, Union[Tuple[typing.Literal[False], str], Tuple[typing.Literal[True], Exception]]] = {}
IO_QUEUE: IOQueueType = Queue()
IS_FINISHED: asyncio.Event = asyncio.Event()
PRINT_EXCEPTIONS: Dict[asyncio.Event, Exception] = {}

# Finished running IO because there's nothing being ran yet.
IS_FINISHED.set()

async def _execute_io() -> None:
    """Helper function for executing IO requests."""
    # Exhaust all of the io requests.
    # Stop if an `IOLock` is currently being used.
    while not IO_QUEUE.empty() and IOLock._class_is_finished.is_set():
        # Get the next io request.
        is_print, event, args, kwargs = await IO_QUEUE.get()
        # Execute the io request in `asyncio`'s default thread.
        if is_print:
            try:
                await asyncio.get_running_loop().run_in_executor(None, partial(print, *args, **kwargs))
            except Exception as e:
                if event is None:
                    logger.exception(e)
                else:
                    PRINT_EXCEPTIONS[event] = e
        else:
            try:
                INPUT_RESULTS[event] = (False, await asyncio.get_running_loop().run_in_executor(None, partial(input, *args)))
            except Exception as e:
                INPUT_RESULTS[event] = (True, e)
        # Signal the io request was completed.
        if event is not None:
            event.set()
        IO_QUEUE.task_done()
    # Signal no io requests are being executed.
    IS_FINISHED.set()

def _schedule_io(is_print: bool, event: Optional[asyncio.Event], args: Tuple[str, ...], kwargs: Optional[PrintKwargs]) -> None:
    """Helper function for scheduling IO requests."""
    # Insert the next IO request.
    IO_QUEUE.put_nowait((is_print, event, args, kwargs))
    # Restart the executor if necessary.
    if IS_FINISHED.is_set() and IOLock._class_is_finished.is_set():
        IS_FINISHED.clear()
        asyncio.create_task(_execute_io())

async def ainput(*args: Any) -> str:
    """
    An asynchronous version of `input`, which runs in a thread.

    Blocks the current coroutine from progressing until `input` is given.

    WARNING:
        Using `ainput` inside of an `IOLock` block will cause deadlock,
        preventing your program from continuing.
        Use `lock.ainput` instead.

        With the default `lock.timeout` however, such deadlocks only
        hold for 10 seconds.

    NOTE:
        Since `ainput` only queues a prompt to be printed evantually,
        it may not print anything if the `asyncio` loop terminates first.
        In order to flush out all remaining `aprint`s and `ainput`s, use
        `aio_stdout.flush`. See `help(aio_stdout.flush)` for more details.
    """
    # Perform early type-checking on args.
    if len(args) > 1:
        raise TypeError(f"ainput expected at most 1 argument, got {len(args)}")
    # Wait for the io to finish.
    is_completed = asyncio.Event()
    # Schedule the `input`.
    _schedule_io(False, is_completed, (*[str(arg) for arg in args],), {})
    # Wait for the `input` to finish.
    await is_completed.wait()
    # Collect the result.
    had_exception, response = INPUT_RESULTS.pop(is_completed)
    if had_exception:
        raise response
    else:
        return response

async def aprint(*args: Any, block: bool = False, **kwargs: Any) -> None:
    """
    An asynchronous version of `print`, which runs in a thread.

    By default, `block=False`, which schedule the `print` but returns
    immediately. If `block=True`, schedule the `print` and wait for it
    to be ran. For example, if an `aprint` occurs after an `ainput`, it
    will wait until the `ainput` is completed to `print` the message,
    but code using the `aprint` has the option to wait for this or not.

    Use `block=True` only if you need the `print` to go through before
    continuing, such as when printing to a file.

    WARNING:
        Using `aprint` with `block=True` inside of an `IOLock` block
        will cause deadlock, preventing your program from continuing.
        Use `lock.aprint` instead.

        Using `aprint` with `block=False` inside of an `IOLock` block
        will delay the `aprint` until the `lock` block is finished.

        With the default `lock.timeout` however, such deadlocks only
        hold for 10 seconds.

    NOTE:
        Since `aprint` only queues a message to be printed evantually,
        it may not print anything if the `asyncio` loop terminates first.
        In order to flush out all remaining `aprint`s and `ainput`s, use
        `aio_stdout.flush`. See `help(aio_stdout.flush)` for more details.
    """
    # Perform early type-checking on kwargs.
    for kwarg, value in kwargs.items():
        if kwarg in ("sep", "end") and value is not None and not isinstance(value, str):
            raise TypeError(f"{kwarg} must be None or a string, not {type(value).__name__}")
        elif kwarg == "file" and not isinstance(value, IO):
            raise TypeError(f"file must be an IO instance, not {type(value).__name__}")
        elif kwarg not in ("sep", "end", "file", "flush"):
            raise TypeError(f"{kwarg!r} is an invalid keyword argument for aprint()")
    # Wait for the io to finish depending on `block`.
    event = asyncio.Event() if block else None
    # Schedule the `print`.
    _schedule_io(True, event, (*[str(arg) for arg in args],), kwargs)
    # Wait for the `print` to finish.
    if block:
        await event.wait()
        if event in PRINT_EXCEPTIONS:
            raise PRINT_EXCEPTIONS.pop(event)
    # Wait at least once before returning so that the print can start running.
    else:
        await asyncio.sleep(0)

# Finalize type-hints.
IOLock.acquire.__annotations__ = {
    "self": Self,
    "return":
        bool if sys.version_info < (3, 8)
        else typing.Literal[True],
}

for hinted in (
    IOLock,
    IOLock.__init__,
    IOLock.__aenter__,
    IOLock.release,
    IOLock._IOLock__exhaust_queue.__func__,
    IOLock._IOLock__wait_event.__func__,
    IOLock._execute_io.__func__,
    IOLock._schedule_io,
    IOLock.ainput,
    IOLock.aprint,
    IOLock.n.fget,
    IOLock.timeout.fget,
    Flush.__aenter__,
    Flush.__aexit__,
    ainput,
    aprint,
):
    hinted.__annotations__ = get_type_hints(hinted)
