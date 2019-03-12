from threading import Thread, Event
import logging

log = logging.getLogger(__name__)


class StoppableThread(Thread):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore
        self.quit_event = Event()
        super().__init__(*args, **kwargs)

    def stop(self) -> None:
        if not self.isAlive():
            return
        self.quit_event.set()
        self.join()
