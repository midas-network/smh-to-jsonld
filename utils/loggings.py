import logging
import os
import sys


class ColoredFormatter(logging.Formatter):
    """Custom formatter that adds colors to log levels."""

    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
    }
    RESET = '\033[0m'

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{log_color}{record.levelname}{self.RESET}"
        return super().format(record)


def setup_logging(data_dir="data"):
    """Setup logging to both console and file."""
    os.makedirs(data_dir, exist_ok=True)
    log_file = os.path.join(data_dir, "last_run.log")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # File handler (no colors)
    file_handler = logging.FileHandler(log_file, mode='w')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    # Console handler (with colors)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ColoredFormatter('%(levelname)s - %(message)s'))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger