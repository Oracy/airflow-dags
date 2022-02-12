from functools import wraps
from time import time


def gen_cicle(to_wakeup, cicle_to_wakeup):
    for cicle in range(0, 6):
        date = to_wakeup - cicle_to_wakeup * cicle
        to_wakeup_str = to_wakeup.strftime("%Y-%m-%d %H:%M:%S.%f")[11:-7]
        if date != to_wakeup:
            yield f"Dormir as: {date.strftime('%Y-%m-%d %H:%M:%S.%f')[11:-7]},' \
            'para acordar as: {to_wakeup_str}, sono total de: {to_wakeup - date}"


def performance(task_id):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            t1 = time()
            res = f(*args, **kwargs)
            t2 = time()
            print(f"{task_id} - Took { t2 - t1 } ms to execute")
            return res

        return wrapper

    return decorator
