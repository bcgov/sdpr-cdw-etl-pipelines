import asyncio
from collections import deque
import tracemalloc
import datetime

tracemalloc.start()


class AsyncWorker:
    """
    Manages asynchronous tasks with a specified concurrency limit.

    This class allows for the addition of asynchronous tasks while controlling
    the number of concurrently running tasks. When the limit is reached, 
    additional tasks are queued until space becomes available.

    Args:
        task_count (int): The maximum number of concurrent tasks. Defaults to 10.
        start_task_sleep_time (float): Time to wait before starting a new task. Defaults to 1 second.
    """

    def __init__(self, task_count: int = 10, start_task_sleep_time: float = 1):
        self.task_count = task_count
        self.running = set()
        self.waiting = deque()
        self.start_task_sleep_time = start_task_sleep_time

    @property
    def running_task_count(self):
        """
        Returns the number of currently running tasks.

        Returns:
            int: The count of currently running tasks.
        """
        return len(self.running)

    async def add_task(self, coro, task_name):
        """
        Adds a new task to the worker. If the limit of running tasks is reached,
        the task will be queued.

        Args:
            coro (Awaitable): The asynchronous callable to be executed as a task.
            task_name (str): A name for the task, used for logging and identification.

        Raises:
            ValueError: If the task name is not unique (i.e., already running).
        """
        if len(self.running) >= self.task_count:
            self.waiting.append(coro)
        else:
            self._start_task(coro, task_name)
            await asyncio.sleep(self.start_task_sleep_time)

    def _start_task(self, coro, task_name):
        """
        Starts a new task and adds it to the running set.

        Args:
            coro (Awaitable): The asynchronous callable to be executed as a task.
            task_name (str): A name for the task, used for logging and identification.
        """
        self.running.add(coro)
        asyncio.create_task(self._task(coro, task_name), name=task_name)

    async def _task(self, coro, task_name):
        """
        Executes the given task and manages its lifecycle.

        Args:
            coro (Awaitable): The asynchronous callable to be executed.
            task_name (str): A name for the task, used for logging and identification.
        """
        try:
            return await coro
        finally:
            self.running.remove(coro)
            if self.waiting:
                coro2 = self.waiting.popleft()
                self._start_task(coro2, task_name)
