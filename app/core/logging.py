import sys
import logging
from logging.handlers import RotatingFileHandler
import os

def setup_logging():
    # Fix Windows console encoding
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding='utf-8')
            sys.stderr.reconfigure(encoding='utf-8')
        except AttributeError:
            # Fallback for older Python versions
            import io
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    else:
        # On Linux/Mac, ensure UTF-8
        import locale
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

    # Create logs directory
    logs_dir = "logs"
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    # Root logger configuration with UTF-8 support
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Clear any existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Console handler with UTF-8 encoding
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    root_logger.addHandler(console_handler)

    # File handler with UTF-8 encoding (rotating)
    try:
        file_handler = RotatingFileHandler(
            "logs/arbicore.log",
            maxBytes=10_485_760,  # 10MB
            backupCount=5,
            encoding='utf-8'  # Critical for Persian characters
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        root_logger.addHandler(file_handler)
    except Exception as e:
        print(f"Warning: Could not create file handler: {e}")

    # Suppress verbose access logs from uvicorn
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    # Test log to confirm Persian support
    logging.info("✅ Logging initialized with UTF-8 encoding")

    # Create a dedicated logger for exchange API errors with full UTF-8
    exchange_logger = logging.getLogger("exchange_errors")
    exchange_logger.setLevel(logging.INFO)
    if not exchange_logger.handlers:
        try:
            error_file_handler = RotatingFileHandler(
                "logs/exchange_errors.log",
                maxBytes=5_242_880,  # 5MB
                backupCount=3,
                encoding='utf-8'
            )
            error_file_handler.setFormatter(logging.Formatter(
                "%(asctime)s %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            ))
            exchange_logger.addHandler(error_file_handler)
        except Exception:
            pass