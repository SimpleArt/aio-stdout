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
    async with IOLock(n=5) as io_lock:
        for i in range(n, 0, -1):
            await io_lock.aprint(i)
            await asyncio.sleep(1)

async def get_name() -> str:
    """Ask the user for their name."""
    async with IOLock() as io_lock:
        name = await io_lock.ainput("What is your name? ")
        await io_lock.aprint(f"Your name is {name}.")
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

async def main() -> None:
    async with flush:
        pass
```

Common Gotchas
---------------

- Using `input` or `print` instead of `ainput` and `aprint` will push
  a message immediately to the console, potentially conflicting with
  `ainput` or `aprint`.
- Using `ainput` or `aprint` instead of `io_lock.ainput` and
  `io_lock.aprint` may produce **deadlock** due to having to
  wait for the lock to release. As such, the `io_lock` is equipped
  with a default `timeout` limit of 10 seconds to avoid deadlock
  and explain to users this potential problem.
'''
from __future__ import annotations
from asyncio import Queue
from enum import Enum
from functools import partial
from typing import Any, ClassVar, Dict, Generic, IO, Literal, Optional, Tuple, Type, TypedDict, TypeVar

import asyncio
import logging
import sys

__all__ = ["IOLock", "ainput", "aprint", "flush"]

T = TypeVar("T")

# Make `asyncio.Queue` generic for type-hinting.
if sys.version_info < (3, 9):

    class Queue(Queue, Generic[T]):
        """
        A queue, useful for coordinating producer and consumer coroutines.

        If maxsize is less than or equal to zero, the queue size is infinite. If it
        is an integer greater than 0, then "await put()" will block when the
        queue reaches maxsize, until an item is removed by get().

        Unlike the standard library Queue, you can reliably know this Queue's size
        with qsize(), since your single-threaded asyncio application won't be
        interrupted between calling qsize() and doing an operation on the Queue.
        """
        __slots__ = ()

        async def get(self: Queue[T], /) -> T:
            """
            Remove and return an item from the queue.

            If queue is empty, wait until an item is available.
            """
            return await super().get()

        def get_nowait(self: Queue[T], /) -> T:
            """
            Remove and return an item from the queue.

            Return an item if one is immediately available, else raise QueueEmpty.
            """
            return super().get_nowait()

        async def put(self: Queue[T], item: T, /) -> T:
            """
            Put an item into the queue.

            Put an item into the queue. If the queue is full, wait until a free
            slot is available before adding item.
            """
            return await super().put(item)

        def put_nowait(self: Queue[T], item: T, /) -> T:
            """
            Put an item into the queue without blocking.

            If no free slot is immediately available, raise QueueFull.
            """
            return super().put_nowait(item)


logger = logging.getLogger(__name__)


class PrintKwargs(TypedDict, total=False):
    sep: Optional[str]
    end: Optional[str]
    file: IO
    flush: Any


IOQueueType = Queue[Tuple[bool, Optional[asyncio.Event], Tuple[str, ...], PrintKwargs]]


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
        >>> io_lock = IOLock(n=..., timeout=...)
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
        >>> async with IOLock() as io_lock:
        ...     name = await io_lock.ainput("What is your name? ")
        ...     await io_lock.aprint(f"Your name is {name}.")
        ...
        What is your name? (...)
        Your name is (...).

    WARNING
    --------
    Using `aprint` with `block=True` or `ainput` inside of an `io_lock`
    block will cause deadlock, preventing your program from continuing.
    Use `io_lock.ainput` and `io_lock.aprint` instead.

    Using `aprint` with `block=False` inside of an `io_lock` block
    will delay the `aprint` until the `io_lock` block is finished.

    With the default `io_lock.timeout` however, such deadlocks only hold for 10 seconds.
    """
    _class_is_finished: ClassVar[asyncio.Event] = asyncio.Event()
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

    def __init__(self: IOLock, /, *args: Any, n: Optional[int] = None, timeout: Optional[float] = 10, **kwargs: Any) -> None:
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

    async def __aenter__(self: IOLock, /) -> IOLock:
        """Acquire the lock and return itself."""
        await super().__aenter__()
        return self

    async def acquire(self: IOLock, /) -> Literal[True]:
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

    def release(self: IOLock, /) -> None:
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
    async def __exhaust_queue(cls: Type[IOLock], io_queue: IOQueueType, /) -> None:
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
    async def __wait_event(cls: Type[IOLock], event: asyncio.Event, message: str, /) -> str:
        """Helper method to wait until an event occurs."""
        await event.wait()
        return message

    @classmethod
    async def _execute_io(cls: Type[IOLock], /) -> None:
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
                tasks = [is_finished_task]
                tasks.append(asyncio.create_task(cls.__wait_event(is_awake, "awake")))
                if timeout is None:
                    as_completed = asyncio.as_completed(tasks)
                else:
                    as_completed = asyncio.as_completed(tasks, timeout=timeout)
                # Wait until one of the tasks is done.
                for task in as_completed:
                    try:
                        task_type = await task
                    except asyncio.TimeoutError:
                        task_type = "timeout"
                    break
                del tasks[0]
                for task in tasks:
                    task.cancel()
                for task in tasks:
                    try:
                        await task
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
                        "An `io_lock` timed out after 10 seconds or more.",
                        "This is likely due to the use of `aprint` or `ainput`",
                        "instead of `io_lock.aprint` or `io_lock.ainput` while",
                        "inside of an `io_lock` block."
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

    def _schedule_io(self: IOLock, is_print: bool, event: Optional[asyncio.Event], args: Tuple[str, ...], kwargs: Optional[PrintKwargs], /) -> None:
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

    async def ainput(self: IOLock, /, *args: Any) -> str:
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

    async def aprint(self: IOLock, /, *args: Any, block: bool = False, **kwargs: Any) -> None:
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
    def n(self: IOLock, /) -> Optional[int]:
        """
        The number of io requests that can be queued at a time
        before letting other io requests go through.

        If `None`, then it blocks until all locked io requests go through.
        """
        return self._n

    @property
    def timeout(self: IOLock, /) -> Optional[float]:
        """
        The number of seconds the io lock can sleep before letting other
        io requests go through.

        If `None`, then it blocks until all locked io requests go through.
        """
        return self._timeout


class Flush(Enum):
    """Use `async with flush: ...` to flush all io before exiting."""
    flush = ()

    async def __aenter__(self: Flush, /) -> None:
        pass

    async def __aexit__(self: Flush, /, *args: Any) -> None:
        """Waits until all IO is flushed."""
        await IOLock._class_is_finished.wait()
        await IS_FINISHED.wait()


flush: Flush = Flush.flush

INPUT_RESULTS: Dict[asyncio.Event, Union[Tuple[Literal[False], str], Tuple[Literal[True], Exception]]] = {}
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

def _schedule_io(is_print: bool, event: Optional[asyncio.Event], args: Tuple[str, ...], kwargs: Optional[PrintKwargs], /) -> None:
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
        Using `ainput` inside of an `io_lock` block will cause deadlock,
        preventing your program from continuing.
        Use `io_lock.ainput` instead.

        With the default `io_lock.timeout` however, such deadlocks only
        hold for 10 seconds.

    NOTE:
        Since `ainput` only queues a prompt to be printed evantually,
        it may not print anything if the `asyncio` loop terminates first.
        In order to flush out all remaining `aprint`s and `ainput`s, use
            >>> async with flush:
            ...     pass  # Main code.
            ...
        at the end of the main code to wait until all other code gets to print.
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
        Using `aprint` with `block=True` inside of an `io_lock` block
        will cause deadlock, preventing your program from continuing.
        Use `io_lock.aprint` instead.

        Using `aprint` with `block=False` inside of an `io_lock` block
        will delay the `aprint` until the `io_lock` block is finished.

        With the default `io_lock.timeout` however, such deadlocks only
        hold for 10 seconds.

    NOTE:
        Since `aprint` only queues a message to be printed evantually,
        it may not print anything if the `asyncio` loop terminates first.
        In order to flush out all remaining `aprint`s and `ainput`s, use
            >>> async with flush:
            ...     pass  # Main code.
            ...
        at the end of the main code to wait until all other code gets to print.
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
