
from enum import Enum
import logging
from logging.handlers import TimedRotatingFileHandler
import os

def get_logger(name: str) -> logging.Logger:
    os.makedirs("logs", exist_ok=True)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = TimedRotatingFileHandler("logs/server.log", when="midnight", interval=1)
    handler.suffix = "%Y-%m-%d" 
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

