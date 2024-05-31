import logging

class Logger:
    def logger(name, log_level) -> logging.Logger:
        match log_level:
            case "debug":
                log_level = logging.DEBUG
            case "info":
                log_level = logging.INFO
            case "warning":
                log_level = logging.WARNING
            case "error":
                log_level = logging.ERROR
            case "critical":
                log_level = logging.CRITICAL

        # TODO: Create handlers?

        logging.basicConfig(
            level = log_level,
            filename = f"logs/{name}.log",
            format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt = "%Y%m%d_%H:%M:%S"
            )
        return logging.getLogger(f"{name}_logger")

    def timer(name) -> logging.Logger:
        logging.basicConfig(
            level = logging.INFO,
            filename = f"logs/_timers.log",
            format = "%(asctime)s | %(message)s",
            datefmt = "%Y%m%d_%H:%M:%S"
            )
        return logging.getLogger(f"{name}_logger")