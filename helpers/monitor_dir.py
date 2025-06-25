import sys
import logging
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

if __name__ == "__main__":

    # path = sys.argv[1] if len(sys.argv) > 1 else '.'
    path = "/tmp/leveldb"
    event_handler = LoggingEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while observer.is_alive():
            observer.join(1)
    finally:
        observer.stop()
        observer.join()
