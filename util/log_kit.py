"""
QuantClass Simons' Log Kit
"""

import sys
from loguru import logger

logger.remove()
logger_format = (
    "<green>{time:HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name: <20}</cyan>:<cyan>{function: <30}</cyan>:<cyan>{line: <4}</cyan> | "
    "<level>{message}</level>"
)
logger.add(sys.stderr, format=logger_format, level="INFO")

