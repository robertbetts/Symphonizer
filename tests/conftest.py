import logging

logging.getLogger("pika").setLevel(logging.WARNING)
logging.getLogger("nuropb").setLevel(logging.INFO)
logging.getLogger("urllib").setLevel(logging.WARNING)
