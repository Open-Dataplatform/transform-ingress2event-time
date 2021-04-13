"""
Contains the configuration.
"""
import configparser
from configparser import ConfigParser
import logging.config
from logging import Logger


class Configuration:
    """
    Contains methods to obtain configurations for this application.
    """

    def __init__(self, name: str):
        self.config = configparser.ConfigParser()
        self.config.read(['conf.ini', '/etc/osiris/conf.ini', '/etc/transform-ingress2event-time-conf.ini'])

        self.credentials_config = configparser.ConfigParser()
        self.credentials_config.read(['credentials.ini', '/vault/secrets/credentials.ini'])

        logging.config.fileConfig(fname=self.config['Logging']['configuration_file'], disable_existing_loggers=False)

        self.name = name

    def get_config(self) -> ConfigParser:
        """
        The configuration for the application.
        """
        return self.config

    def get_credentials_config(self) -> ConfigParser:
        """
        The credential config for the application.
        """
        return self.credentials_config

    def get_logger(self) -> Logger:
        """
        A customized logger.
        """
        return logging.getLogger(self.name)
