import logging
import colorlog


class CustomFormatter(logging.Logger):
    """
    CustomFormatter is a subclass of logging.Logger that extends its functionality by introducing
    new log levels called 'SUCCESS' and 'DANGER'. It also provides methods to log messages with these levels.
    """

    # Define custom log levels named 'SUCCESS' and 'DANGER' with values between INFO(20) and WARNING(30)
    SUCCESS = 25
    DANGER = 28

    def __init__(self, name: str):
        """
        Initializes a new instance of CustomFormatter with the given name.

        Args:
            name (str): Name of the logger.
        """
        super().__init__(name)

        # Register the custom log levels 'SUCCESS' and 'DANGER' in the global logging module
        logging.addLevelName(self.SUCCESS, "SUCCESS")
        logging.addLevelName(self.DANGER, "DANGER")

    def success(self, msg: str, *args, **kwargs) -> None:
        """
        Log a message with severity 'SUCCESS'.

        Args:
            msg (str): The message format string.
            args: Arguments merged into msg using the string formatting operator.
            kwargs: A dictionary containing additional keyword arguments for the logger.
        """
        if self.isEnabledFor(self.SUCCESS):
            # Log the message with the custom SUCCESS level
            self._log(self.SUCCESS, msg, args, **kwargs)

    def danger(self, msg: str, *args, **kwargs) -> None:
        """
        Log a message with severity 'DANGER'.

        Args:
            msg (str): The message format string.
            args: Arguments merged into msg using the string formatting operator.
            kwargs: A dictionary containing additional keyword arguments for the logger.
        """
        if self.isEnabledFor(self.DANGER):
            # Log the message with the custom DANGER level
            self._log(self.DANGER, msg, args, **kwargs)


# Create a CustomFormatter logger named 'opc_client' with DEBUG level enabled
logger = CustomFormatter("dc streaming")
logger.setLevel(logging.DEBUG)

# Create a console handler for logging, and set its level to DEBUG
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create a color formatter to implement colored logging output
color_formatter = colorlog.ColoredFormatter(
    # Define the log format string, including log color
    "%(asctime)s - %(name)s - %(log_color)s%(levelname)s%(reset)s - %(message)s (%(filename)s:%(lineno)d)",
    # Define the colors for different log levels including the custom SUCCESS level
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'cyan',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
        'SUCCESS': 'bold_green',
        'DANGER': 'bold_purple'
    },
    reset=True,
    style='%'
)

# Assign the color formatter to the console handler
console_handler.setFormatter(color_formatter)

# Add the console handler to the logger
logger.addHandler(console_handler)
