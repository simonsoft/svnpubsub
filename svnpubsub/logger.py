import sys
import logging
import logging.handlers


def setup(logfile=None, level=logging.INFO):

    if logfile:
        # Rotate logs daily, keeping 7 days worth.
        handler = logging.handlers.TimedRotatingFileHandler(logfile, when='midnight', backupCount=7)
    else:
        handler = logging.StreamHandler(sys.stdout)

    # Add a timestamp to the log records
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)

    # Apply the handler to the root logger
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(level)

    return root
