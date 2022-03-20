import logging as lg, configHandler as cfg
import os
from os import path
from logging.handlers import RotatingFileHandler

class customLogger:

    def __init__(self,appname='main',ipaddress=None,username=None):
        try:

            self.appname = appname
            if(ipaddress is not None and username is not None):
                format = '%(asctime)-15s %(name)s %(clientip)s %(user)-8s %(message)s'
                self.extra = True
            else:
                format = '%(asctime)-15s %(name)s %(message)s'
                self.extra = False

            self.log_formatter = lg.Formatter(format)

            ch = cfg.configHandler("config.ini")
            options = ch.readConfigSection("log")

            self.file_name = options['file_name']
            self.file_name2 = options['file_name2']
            self.level = options['level']
            self.level2 = options['level2']
            self.max_bytes = options['max_bytes']
            self.backup_count = options['backup_count']
            self.d = {'clientip': ipaddress, 'user': username}

            # Deleting all the old log files for first time from parent directory
            self.flushLogFiles()

            self.logger = lg.getLogger(self.appname)

            # lg.basicConfig(filename=self.file_name , format=self.log_formatter)

            # Below function replace above commented lines to implement rotating log file handler
            self.addRotatingFileHandler()
            self.addConsoleLogger()

            if self.level == "DEBUG":
                self.logger.setLevel(lg.DEBUG)
            elif self.level == "ERROR":
                self.logger.setLevel(lg.ERROR)
            elif self.level == "WARNING":
                self.logger.setLevel(lg.WARNING)
            elif self.level == "INFO":
                self.logger.setLevel(lg.INFO)

        except Exception as e:
            print('Logger Failed with exception - {}'.format(e))

    def flushLogFiles(self):
        """
        will derive parent directory and will delete all .log files
        :return:
        """
        try:

            if path.exists(self.file_name):
                directory = os.path.dirname(self.file_name)
                for f in os.listdir(directory):
                    if not f.endswith(".log") and f.find('.log.') == -1:
                        continue
                    os.remove(os.path.join(directory, f))
        except Exception as e:
            print('Console Logger Failed with exception - {}'.format(e))

    def addConsoleLogger(self):

        try:
            # To add console stream handler
            console_log = lg.StreamHandler()
            console_log.setLevel(lg.DEBUG)
            console_log.setFormatter(self.log_formatter)
            self.logger.addHandler(console_log)
        except Exception as e:
            print('Console Logger Failed with exception - {}'.format(e))


    def addRotatingFileHandler(self):
        """
        To create new log file after provided max file size reached, also have limit defined for number of backup files
        :return:
        """

        try:

            rotating_handler = lg.handlers.RotatingFileHandler(self.file_name, mode='w',maxBytes=int(self.max_bytes), backupCount=int(self.backup_count), encoding="utf-8",delay=False)
            rotating_handler.setFormatter(self.log_formatter)

            # Uncomment to create new log file on each run and save old one in rotating manner
            # should_roll_over = path.isfile(self.file_name)
            # if should_roll_over:  # log already exists, roll over!
            #     rotating_handler.doRollover()

            self.logger.addHandler(rotating_handler)

            if(self.level2 is not None and self.level2.strip() != ""
               and self.file_name2 is not None and self.file_name2.strip() != ""):
                self.additionalRotatingFileHandler() # Can use to Create Separate Error Log file if Main Log file have Log level INFO

        except Exception as e:
            print('Rotating File Handler Failed with exception - {}'.format(e))

    def additionalRotatingFileHandler(self):
        """
        To Create Separate Log file with different logging level than the one defined for main Log file
        :return:
        """
        try:

            rotating_handler2 = RotatingFileHandler(self.file_name2, maxBytes=int(self.max_bytes), backupCount=int(self.backup_count),encoding="utf-8")
            rotating_handler2.setFormatter(self.log_formatter)

            if self.level2 == "DEBUG":
                rotating_handler2.setLevel(lg.DEBUG)
            elif self.level2 == "ERROR":
                rotating_handler2.setLevel(lg.ERROR)
            elif self.level2 == "WARNING":
                rotating_handler2.setLevel(lg.WARNING)
            elif self.level2 == "INFO":
                rotating_handler2.setLevel(lg.INFO)

            self.logger.addHandler(rotating_handler2)

        except Exception as e:
            print('Rotating Error File Handler Failed with exception - {}'.format(e))

    def log(self,msg,level="INFO"):
        '''

        :param msg: msg to log
        :param level: default is "INFO" but can provide "WARNING"/"ERROR"
        :return: nothing
        '''
        try:
            if(self.extra):
                if level == "INFO":
                    self.logger.info(msg , extra=self.d)
                elif level == "WARNING":
                    self.logger.warning(msg , extra=self.d)
                else:
                    self.logger.error(msg, extra=self.d)
            else:
                if level == "INFO":
                    self.logger.info(msg)
                elif level == "WARNING":
                    self.logger.warning(msg)
                else:
                    self.logger.error(msg)
        except Exception as e:
            print('Log function Failed with exception - {}'.format(e))

# for debugging purpose
if(__name__ == "__main__"):

    clg = customLogger(__name__)
    clg.log('Random log msg for test INFO')
    clg.log('Random log msg for test WARNING','WARNING')
    clg.log('Random log msg for test ERROR','ERROR')

    for i in range(0, 1000):
        clg.log('INFO This is a message {}'.format(i))
        if i % 5 == 0:
            clg.log('ERROR THis is a error {}'.format(i),'ERROR')

    # To create logs with extra info
    # clg2 = customLogger(__name__, '192.168.0.1', 'WebAPP')
    # clg2.log('Random log msg for test2')
    # clg2.log('Random log msg for test2', 'WARNING')
    # clg2.log('Random log msg for test2', 'ERROR')



