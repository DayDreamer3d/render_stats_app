""" Module for caching.
"""
import ast
import os

import config
from _impl.utils import log, utils





class Cache(object):

    def __init__(self, cache_file=config.Cache.persistence_path):
        self.cache_file = cache_file
        self.cache_items = {}

        # create an empty file
        if not os.path.isfile(self.cache_file):
            open(self.cache_file, 'w').close()

    def set(self, cmd, value):
        key = get_hash(cmd)
        item = {key: value}
        self.cache_items.update(item)
        with open(self.cache_file, 'a') as cache_file:
            cache_file.write('{}\n'.format(item))
        log.log.info('CACHE SET for ITEM({})'.format(item))
        return True

    def get(self, cmd):
        key = get_hash(cmd)
        value = self.cache_items.get(key)
        if not value:
            with open(self.cache_file, 'r') as cache_file:
                for line in cache_file.readlines():
                    if not key in line:
                        continue
                    item = ast.literal_eval(line)
                    log.log.info('CACHE GET from FILE with KEY({}): VALUE({})'.format(key, item[key]))
                    return item[key]
        log.log.info('CACHE GET for KEY({}): VALUE({})'.format(key, value))


def get_hash(cmd):
    """ Get hash key from args

        Args:
            cmd (Command): command object from which we ca retieve required args.

        Returns
            str: hash value in hex.
    """
    # only use the filename for hash as it's location can change
    # which will change the absolute path in turn the hash value
    # but the content of the file remains the same.
    filename = os.path.basename(cmd.logs_dir)
    filter_args = sorted(cmd.filter_args)
    aggregator_args = sorted(cmd.aggregator_args)

    seq = [filename, filter_args, aggregator_args]
    log.log.info('CACHE SEQ({})'.format(seq))
    hash_ = utils.hash_(seq)

    return hash_
