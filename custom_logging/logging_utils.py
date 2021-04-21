import os
import datetime
import logging
import logging.config


def configure_logger():
    # Defing logging configurations
    log_level = os.environ.get("LOGLEVEL", "INFO")
    log_handler_list = ["CONSOLE", "FILE", "SPLUNK"]
    log_handler = log_handler_list[0:2]

    if log_handler == "FILE":
        logs_path = "logs/"
        if not os.path.isdir(logs_path):
            os.mkdir(logs_path)
        run_time = str(datetime.datetime.now().replace(microsecond=0))
        run_time = run_time.translate(str.maketrans({" ": "_", "-": "_", ":":"_"}))
        log_file_name = f"custom_logger{run_time}.log"
        log_file_path = os.path.join(logs_path,log_file_name)
    else:
        log_file_path = "./custom_logger.log"


    logging_config = { 
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": { 
            "standard": {
                "format":"[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
            },
        },
        "handlers": { 
            "CONSOLE": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
                "formatter": "standard",
        
                "stream": "ext://sys.stdout",
            },
            "FILE": {
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "standard",
                "level": "INFO",
                "filename": log_file_path,
                "maxBytes": 10240,
                "backupCount": 3,
            },
            'SPLUNK': { # handler for splunk, level Warning. to not have many logs sent to splunk
                'class': 'splunk_handler.SplunkHandler',
                'level': 'WARNING',
                'formatter': 'standard',
                'url': os.getenv('SPLUNK_HTTP_COLLECTOR_URL'),
                'host': os.getenv('SPLUNK_HOST'),
                'port': os.getenv('SPLUNK_PORT'),
                'token': os.getenv('SPLUNK_TOKEN'),
                'index': os.getenv('SPLUNK_INDEX')         
            },
        },
        "loggers": { 
            "": {  # For root logger
                "handlers": log_handler,
                "level": log_level,
                "propagate": False
            },
            "custom-logger": { # For "custom-logger"
                "handlers": log_handler,
                "level": log_level,
                "propagate": False
            },
            "__main__": { # if __name__ == "__main__"
                "handlers": log_handler,
                "level": log_level,
                "propagate": False
            },
        } 
    }

    # Se the logging configurations
    logging.config.dictConfig(logging_config)
    logger = logging.getLogger("custom-logger")
    if log_level == "INFO":
        logger.info("Logging is configured with level: INFO.")
    elif log_level == "DEBUG":
        logger.debug("Logging is configured with level: DEBUG")
    
    return logger


logger = configure_logger()
