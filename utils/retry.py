from functools import wraps
from typing import Callable, Any
from time import sleep

def retry(retries: int = 3, delay: float = 1) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            for i in range(1, retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if i == retries:
                        print(f'Error: {repr(e)}.')
                        print(f'"{func.__name__}()" failed after {retries} retries.')
                        break
                    else:
                        sleep(delay)
        return wrapper
    return decorator