from functools import wraps
from time import sleep


def retry(numRetries=5, retryDelay=3, backoffScalingFactor=2, logger=None):

    def retry_decorator(func):
        @wraps(func)
        def retry_function(*args, **kwargs):
            numTries, currentDelay = numRetries, retryDelay
            while numTries > 1:
                try:
                    return func(*args, **kwargs)
                except Exception as ex:
                    exceptionMessage = f'{ex}, Retrying in {currentDelay} seconds...'
                    if logger:
                        logger.warning(exceptionMessage)
                    else:
                        print(exceptionMessage)
                    sleep(currentDelay)
                    numTries -= 1
                    currentDelay *= backoffScalingFactor
            return func(*args, **kwargs)
        return retry_function
    return retry_decorator
