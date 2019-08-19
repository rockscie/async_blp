from typing import Iterable
from typing import List
from typing import TypeVar

T = TypeVar('T')


def split_into_chunks(iterable: List[T],
                      chunk_size: int,
                      ) -> Iterable[List[T]]:
    """
    Split given iterable into several iterables of size `chunk_size` or smaller
    """
    num_chunks = len(iterable) // chunk_size

    if len(iterable) % chunk_size != 0:
        num_chunks += 1

    for i in range(num_chunks):
        yield iterable[i * chunk_size: (i + 1) * chunk_size]
