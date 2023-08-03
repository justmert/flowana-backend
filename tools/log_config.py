import logging
import os

log_file = os.path.join(os.path.dirname(__file__), "flowana.log")

logging.basicConfig(
    filename=log_file,
    level=logging.DEBUG,
    format="%(asctime)s - %(filename)s - %(message)s",
    filemode="w",
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(filename)s - %(message)s"))
logging.getLogger().addHandler(console_handler)
