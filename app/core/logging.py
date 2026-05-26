import logging
import sys

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("arbicore.log")  # add this
        ]
    )
    # Optionally set specific log levels
    logging.getLogger("uvicorn").setLevel(logging.INFO)