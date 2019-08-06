"""
abstract Handler for typing
"""

import abc
from typing import List


class AbsHandler(metaclass=abc.ABCMeta):
    """
    All Handler must have session and __call__
    """

    def __init__(self):
        self.session = None

    @abc.abstractmethod
    def __call__(self, event, session):
        pass

    @abc.abstractmethod
    def send_requests(self, requests: List):
        """
        save and prepare requests
        """
