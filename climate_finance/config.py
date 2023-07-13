import logging
from pathlib import Path


class ClimateDataPath:
    """Class to store the paths to the data and output folders."""

    project = Path(__file__).resolve().parent.parent
    scripts = project / "climate_finance"
    raw_data = scripts / ".raw_data"


# Create a root logger
logger = logging.getLogger(__name__)

# Create two handlers (terminal and file)
shell_handler = logging.StreamHandler()

# Set levels for the logger, shell and file
logger.setLevel(logging.DEBUG)
shell_handler.setLevel(logging.DEBUG)

# Format the outputs
fmt_file = "%(levelname)s (%(asctime)s): %(message)s"
fmt_shell = (
    "%(levelname)s %(asctime)s [%(filename)s:%(funcName)s:%(lineno)d] %(message)s"
)

# Create formatters
shell_formatter = logging.Formatter(fmt_shell)
file_formatter = logging.Formatter(fmt_file)

# Add formatters to handlers
shell_handler.setFormatter(shell_formatter)

# Add handlers to the logger
logger.addHandler(shell_handler)
