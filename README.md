# aio_stdout
 Asynchronous Input Output - Stdout

The purpose of this package is to provide asynchronous variants of the builtin `input` and `print` functions. `print` is known to be relatively slow compared to other operations. `input` is even slower because it has to wait for user input. While these slow IO operations are being ran, code using `asyncio` should be able to continuously run.

PIP Installing
---------------

For Unix/macOS:
```
python3 -m pip install aio-stdout
```

For Windows:
```
py -m pip install aio-stdout
```

ainput and aprint
------------------

With `aio_stdout`, the `aio_stdout.ainput` and `aio_stdout.aprint` functions provide easy to use functionality with organized behaviour.

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

Notice that while the prompt `"What is your name? "` is being waited for, the `countdown` continues to `aprint` in the background, without becoming blocked. The `countdown` does not, however, display its results until the `ainput` is completed. Instead it waits for the `ainput` to finish before flushing out all of the queued messages.

It is worth noting that with naive threading, a normal attempt to use `print` while waiting on an `input` leads to overlapping messages. Fixing this behavior requires a lot more work than should be needed to use a simple `print` or `input` function, which is why this package exists. To remedy this problem, queues are used to store messages until they are ready to be printed. 

IO Locks
---------

Although the asynchronization behaviors of `ainput` and `aprint` are nice, sometimes we want to be able to synchronize our messages even more. IO locks provide a way to group messages together, locking the global `aio_stdout` queues until it finishes or yields access.

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

Notice that this time the `countdown` does not immediately yield to the `get_name`. Instead, it runs 5 messages before yielding control over to `get_name`. Now, after the `ainput` finishes, it does not yield to `countdown`. Instead, it runs its own `aprint` first. In the meantime, `countdown` continues to run in the background and flushes all of its buffered messages afterwards.

Flushing
---------

Since messages may be delayed, it is possible for your asynchronous code to finish running before all messages are displayed, producing confusing results. As such, the best recommended practice is to flush from `main` before terminating.

```python
from aio_stdout import flush

async def main() -> None:
    async with flush:
        pass
```

Common Gotchas
---------------

- Using `input` or `print` instead of `ainput` and `aprint` will push a message immediately to the console, potentially conflicting with `ainput` or `aprint`.
- Using `ainput` or `aprint` instead of `io_lock.ainput` and `io_lock.aprint` may produce **deadlock** due to having to wait for the lock to release. As such, the `io_lock` is equipped with a default `timeout` limit of 10 seconds to avoid deadlock and explain to users this potential problem.
