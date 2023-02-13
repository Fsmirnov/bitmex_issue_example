from datetime import datetime
from enum import IntEnum


class Level(IntEnum):
    INFO = 1
    DEBUG = 2
    ERROR = 3


class BitMEXLogger:
    def __init__(self, level: Level):
        self.level: Level = level
        self.logger_data: str = ' - Test BitMEX - '
        self.logger_info: str = 'INFO - '
        self.logger_debug: str = 'DEBUG - '
        self.logger_error: str = 'ERROR - '
        # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    def info(self, message: str):
        if self.level is Level.ERROR:
            return
        print_message: str = str(datetime.now()) + self.logger_data + self.logger_info + f'{message}'
        print(print_message)
        with open('test_bitmex.log', 'a') as file:
            file.write(print_message + '\n')

    def debug(self, message: str):
        if self.level is not Level.DEBUG:
            return
        print_message: str = str(datetime.now()) + self.logger_data + self.logger_debug + f'{message}'
        print(print_message)
        with open('test_bitmex.log', 'a') as file:
            file.write(print_message + '\n')

    def error(self, message: str):
        print_message: str = str(datetime.now()) + self.logger_data + self.logger_error + f'{message}'
        print(print_message)
        with open('test_bitmex.log', 'a') as file:
            file.write(print_message + '\n')


logger = BitMEXLogger(Level.DEBUG)
