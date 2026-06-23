import sys
import logging

def setup_logging():
    # Fix Windows console encoding
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding='utf-8')
            sys.stderr.reconfigure(encoding='utf-8')
        except AttributeError:
            pass

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("arbicore.log", encoding='utf-8')  # ensure file uses UTF-8
        ]
    )
    # Suppress verbose access logs
    logging.getLogger("uvicorn.access").setLevel(logging.INFO)