from functools import wraps
import logging
from time import sleep


def retry(numRetries: int = 5, retryDelaySeconds: int = 3, backoffScalingFactor: int = 2):

    def retry_decorator(func):
        @wraps(func)
        def retry_function(*args, **kwargs):
            numTries, currentDelay = numRetries, retryDelaySeconds
            while numTries > 1:
                try:
                    return func(*args, **kwargs)
                except Exception as ex:
                    exceptionMessage = f'{ex}, Retrying in {currentDelay} seconds...'
                    logging.warning(exceptionMessage)
                    sleep(currentDelay)
                    numTries -= 1
                    currentDelay *= backoffScalingFactor
            return func(*args, **kwargs)
        return retry_function
    return retry_decorator
