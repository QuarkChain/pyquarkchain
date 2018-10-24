from abc import ABC, abstractmethod
import asyncio
import functools
import logging
from typing import Any, Awaitable, Callable, List, Optional, cast
from weakref import WeakSet

from eth_utils import ValidationError

from quarkchain.utils import Logger
from quarkchain.p2p.cancel_token.token import CancelToken, OperationCancelled
from quarkchain.p2p.cancellable import CancellableMixin
from quarkchain.p2p.utils import get_asyncio_executor


class ServiceEvents:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.stopped = asyncio.Event()
        self.cleaned_up = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.finished = asyncio.Event()


class BaseService(ABC, CancellableMixin):
    # Use a WeakSet so that we don't have to bother updating it when tasks finish.
    _child_services = None  # : 'WeakSet[BaseService]'
    _tasks = None  # : 'WeakSet[asyncio.Future[Any]]'
    _finished_callbacks = None  # : List[Callable[['BaseService'], None]]
    # Number of seconds cancel() will wait for run() to finish.
    _wait_until_finished_timeout = 5

    # the custom event loop to run in, or None if the default loop should be used
    _loop = None  # : asyncio.AbstractEventLoop

    def __init__(
        self, token: CancelToken = None, loop: asyncio.AbstractEventLoop = None
    ) -> None:
        self.events = ServiceEvents()
        self._run_lock = asyncio.Lock()
        self._child_services = WeakSet()
        self._tasks = WeakSet()
        self._finished_callbacks = []

        self._loop = loop

        base_token = CancelToken(type(self).__name__, loop=loop)

        if token is None:
            self.cancel_token = base_token
        else:
            self.cancel_token = base_token.chain(token)

        self._executor = get_asyncio_executor()

    @property
    def logger(self) -> Logger:
        return Logger

    def get_event_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            return asyncio.get_event_loop()
        else:
            return self._loop

    async def run(
        self, finished_callback: Optional[Callable[["BaseService"], None]] = None
    ) -> None:
        """Await for the service's _run() coroutine.

        Once _run() returns, triggers the cancel token, call cleanup() and
        finished_callback (if one was passed).
        """
        if self.is_running:
            raise ValidationError("Cannot start the service while it's already running")
        elif self.is_cancelled:
            raise ValidationError(
                "Cannot restart a service that has already been cancelled"
            )

        if finished_callback:
            self._finished_callbacks.append(finished_callback)

        try:
            async with self._run_lock:
                self.events.started.set()
                await self._run()
        except OperationCancelled as e:
            self.logger.debug("%s finished: %s" % (self, e))
        except Exception:
            self.logger.exception("Unexpected error in %r, exiting" % self)
        finally:
            # Trigger our cancel token to ensure all pending asyncio tasks and background
            # coroutines started by this service exit cleanly.
            self.events.cancelled.set()
            self.cancel_token.trigger()

            await self.cleanup()

            for callback in self._finished_callbacks:
                callback(self)

            self.events.finished.set()
            self.logger.debug("%s halted cleanly" % self)

    def add_finished_callback(
        self, finished_callback: Callable[["BaseService"], None]
    ) -> None:
        self._finished_callbacks.append(finished_callback)

    def run_task(self, awaitable: Awaitable[Any]) -> None:
        """Run the given awaitable in the background.

        The awaitable should return whenever this service's cancel token is triggered.

        If it raises OperationCancelled, that is caught and ignored.
        """

        @functools.wraps(awaitable)  # type: ignore
        async def _run_task_wrapper() -> None:
            self.logger.debug("Running task %s" % awaitable)
            try:
                await awaitable
            except OperationCancelled:
                pass
            except Exception as e:
                self.logger.warning("Task %s finished unexpectedly: %s" % (awaitable, e))
                self.logger.debug("Task failure traceback")
            else:
                self.logger.debug("Task %s finished with no errors" % awaitable)

        self._tasks.add(asyncio.ensure_future(_run_task_wrapper()))

    def run_daemon_task(self, awaitable: Awaitable[Any]) -> None:
        """Run the given awaitable in the background.

        Like :meth:`run_task` but if the task ends without cancelling, then this
        this service will terminate as well.
        """

        @functools.wraps(awaitable)  # type: ignore
        async def _run_daemon_task_wrapper() -> None:
            try:
                await awaitable
            finally:
                if not self.is_cancelled:
                    self.logger.debug(
                        "%s finished while %s is still running, terminating as well" %
                        (awaitable, self)
                    )
                    self.cancel_token.trigger()

        self.run_task(_run_daemon_task_wrapper())

    def run_child_service(self, child_service: "BaseService") -> None:
        """
        Run a child service and keep a reference to it to be considered during the cleanup.
        """
        if child_service.is_running:
            raise ValidationError(
                "Can't start service {}, child of {}: it's already running".format(repr(child_service), repr(self))
            )
        elif child_service.is_cancelled:
            raise ValidationError(
                "Can't restart {}, child of {}: it's already completed".format(repr(child_service), repr(self))
            )

        self._child_services.add(child_service)
        self.run_task(child_service.run())

    def run_daemon(self, service: "BaseService") -> None:
        """
        Run a service and keep a reference to it to be considered during the cleanup.

        If the service finishes while we're still running, we'll terminate as well.
        """
        if service.is_running:
            raise ValidationError(
                "Can't start daemon {}, child of {}: it's already running".format(repr(service), repr(self))
            )
        elif service.is_cancelled:
            raise ValidationError(
                "Can't restart daemon {}, child of {}: it's already completed".format(repr(service), repr(self))
            )

        self._child_services.add(service)

        @functools.wraps(service.run)
        async def _run_daemon_wrapper() -> None:
            try:
                await service.run()
            except OperationCancelled:
                pass
            except Exception as e:
                self.logger.warning(
                    "Daemon Service %s finished unexpectedly: %s" % (service, e)
                )
                self.logger.debug("Daemon Service failure traceback")
            finally:
                if not self.is_cancelled:
                    self.logger.debug(
                        "%s finished while %s is still running, terminating as well" %
                        (service, self)
                    )
                    self.cancel_token.trigger()

        self.run_task(_run_daemon_wrapper())

    def call_later(
        self, delay: float, callback: "Callable[..., None]", *args: Any
    ) -> None:
        @functools.wraps(callback)
        async def _call_later_wrapped() -> None:
            await self.sleep(delay)
            callback(*args)

        self.run_task(_call_later_wrapped())

    async def _run_in_executor(self, callback: Callable[..., Any], *args: Any) -> Any:
        loop = self.get_event_loop()
        return await self.wait(loop.run_in_executor(self._executor, callback, *args))

    async def cleanup(self) -> None:
        """
        Run the ``_cleanup()`` coroutine and set the ``cleaned_up`` event after the service as
        well as all child services finished their cleanup.

        The ``_cleanup()`` coroutine is invoked before the child services may have finished
        their cleanup.
        """
        if self._child_services:
            self.logger.debug(
                "Waiting for child services: %s" % list(self._child_services)
            )
            await asyncio.gather(
                *[
                    child_service.events.cleaned_up.wait()
                    for child_service in self._child_services
                ]
            )
            self.logger.debug("All child services finished")
        if self._tasks:
            self.logger.debug("Waiting for tasks: %s" % list(self._tasks))
            await asyncio.gather(*self._tasks)
            self.logger.debug("All tasks finished")

        await self._cleanup()
        self.events.cleaned_up.set()

    def cancel_nowait(self) -> None:
        if self.is_cancelled:
            self.logger.warning(
                "Tried to cancel %s, but it was already cancelled" % self
            )
            return
        elif not self.is_running:
            raise ValidationError("Cannot cancel a service that has not been started")

        self.logger.debug("Cancelling %s" % self)
        self.events.cancelled.set()
        self.cancel_token.trigger()

    async def cancel(self) -> None:
        """Trigger the CancelToken and wait for the cleaned_up event to be set."""
        self.cancel_nowait()

        try:
            await asyncio.wait_for(
                self.events.cleaned_up.wait(), timeout=self._wait_until_finished_timeout
            )
        except asyncio.futures.TimeoutError:
            self.logger.info(
                "Timed out waiting for %s to finish its cleanup, forcibly cancelling pending "
                "tasks and exiting anyway" %
                self,
            )
            if self._tasks:
                self.logger.debug("Pending tasks: %s" % list(self._tasks))
            if self._child_services:
                self.logger.debug(
                    "Pending child services: %s" % list(self._child_services)
                )
            self._forcibly_cancel_all_tasks()
            # Sleep a bit because the Future.cancel() method just schedules the callbacks, so we
            # need to give the event loop a chance to actually call them.
            await asyncio.sleep(0.5)
        else:
            self.logger.debug("%s finished cleanly" % self)

    def _forcibly_cancel_all_tasks(self) -> None:
        for task in self._tasks:
            task.cancel()

    @property
    def is_cancelled(self) -> bool:
        return self.cancel_token.triggered

    @property
    def is_operational(self) -> bool:
        return self.events.started.is_set() and not self.cancel_token.triggered

    @property
    def is_running(self) -> bool:
        return self._run_lock.locked()

    async def threadsafe_cancel(self) -> None:
        """
        Cancel service in another thread. Block until service is cleaned up.

        :param poll_period: how many seconds to wait in between each check for service cleanup
        """
        asyncio.run_coroutine_threadsafe(self.cancel(), loop=self.get_event_loop())
        await asyncio.wait_for(
            self.events.cleaned_up.wait(), timeout=self._wait_until_finished_timeout
        )

    async def sleep(self, delay: float) -> None:
        """Coroutine that completes after a given time (in seconds)."""
        await self.wait(asyncio.sleep(delay))

    @abstractmethod
    async def _run(self) -> None:
        """Run the service's loop.

        Should return or raise OperationCancelled when the CancelToken is triggered.
        """
        raise NotImplementedError()

    async def _cleanup(self) -> None:
        """Clean up any resources held by this service.

        Called after the service's _run() method returns.
        """
        pass

    def gc(self) -> None:
        for cs in self._child_services.copy():
            if cs.events.finished.is_set():
                self._child_services.remove(cs)
        for t in self._tasks.copy():
            if t.done():
                self._tasks.remove(t)


def service_timeout(timeout: int) -> Callable[..., Any]:
    """
    Decorator to time out a method call.

    :param timeout: seconds to wait before raising a timeout exception

    :raise asyncio.futures.TimeoutError: if the call is not complete before timeout seconds
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        async def wrapped(service: BaseService, *args: Any, **kwargs: Any) -> Any:
            return await service.wait(func(service, *args, **kwargs), timeout=timeout)

        return wrapped

    return decorator


class EmptyService(BaseService):
    async def _run(self) -> None:
        pass

    async def _cleanup(self) -> None:
        pass
