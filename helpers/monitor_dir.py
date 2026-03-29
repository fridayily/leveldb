import sys
import logging
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s "%(filename)s:%(lineno)d" : %(message)s'

)

# 添加启动日志
logging.info(f"Starting file system monitor on path: /tmp/db_test")
logging.info("Monitoring started. Press Ctrl+C to stop.")
class MyEventHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            logging.info(f"File created: {event.src_path}")

    def on_deleted(self, event):
        if not event.is_directory:
            logging.info(f"File deleted: {event.src_path}")

    def on_modified(self, event):
        if not event.is_directory:
            logging.info(f"File modified: {event.src_path}")

    def on_moved(self, event):
        if not event.is_directory:
            logging.info(f"File moved: {event.src_path} -> {event.dest_path}")

    def on_closed(self, event):
            if not event.is_directory:
                logging.info(f"File closed: {event.src_path}")

    def on_closed_no_write(self, event):
        if not event.is_directory:
            logging.info(f"File closed without writing: {event.src_path}")

if __name__ == "__main__":

    # path = sys.argv[1] if len(sys.argv) > 1 else '.'
    path = "/tmp/db_test"
    event_handler = MyEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while observer.is_alive():
            observer.join(1)
    finally:
        observer.stop()
        observer.join()
