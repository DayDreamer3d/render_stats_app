""" Module containing general uitility functions.
"""

import glob
import hashlib
import os

import config
from _impl.core.orm import engine
from _impl.core.orm import models


def distribution(item_size, workers):
    """ Round robin distribution of items over workers.

        Args:
            item_size (int): size of the items.
            workers (int): number of workers.

        Returns:
            Filter: iterator which can generate items indicies for workers.
    """
    distribution = [[] for _ in range(workers)]
    index = 0
    while index < item_size:
        distribution[index % workers].append(index)
        index += 1
    return filter(bool, distribution)


def collect_data_stores(logs_dir, filename_pattern=config.DataStore.filename_pattern):
    """ Collect all data files from persistent store.

        Args:
            logs_dir (str): directoy containing logs.

        Kwargs:
            filename_pattern (str): pattern to fetch the file names.

        Yield:
                Engine: Orm engine per data store.
    """
    logs_dir = os.path.abspath(logs_dir)
    for render_stats in glob.iglob(os.path.join(logs_dir, filename_pattern)):
        log_file = os.path.join(logs_dir, render_stats)
        yield engine.Engine(log_file, models.RenderStats)


def hash_(seq):
    """ Generate hash from the given sequence.

        Args:
            seq (Sequence): any sequence to generate hash from.

        Returns:
            str : hash value in hex.
    """
    seq.sort()
    seq = [str(s) for s in seq]
    return hashlib.md5(''.join(seq)).hexdigest()
