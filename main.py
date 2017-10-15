#!/usr/bin/env python
""" Main module for the app which interacts with other modules for working

    Basic steps of working
    > get the user inputs
    > collect the files from persistent store
    > retrieve the render records
    > filter the records according to user inputs
    > aggregate the result
    > output the result
"""

import config
from _impl.core.orm import engine
from _impl.core.compute import aggregators, filters
from _impl.core.orm import models
from _impl.core import workers
from _impl.utils import cache, cli, log, utils


def main():
    """ Main function of the app
    """
    # get the cli args
    cmd = cli.Command()
    cmd.parse()

    cache_obj = cache.Cache()
    # query the cache with args
    cache_value = cache_obj.get(cmd)
    if cache_value:
        # output result
        cmd.display_output(cache_value)
        return

    # collect the data stores
    data_stores = list(utils.collect_data_stores(cmd.logs_dir))
    if not data_stores:
        log.log.error('No data stores found in {}'.format(cmd.logs_dir))
        return

    # convert args to objects
    filters_objs = [
        filters.FilterFactory.create(name, value)
        for name, value in cmd.filter_args
    ]

    aggregator_objs = [
        aggregators.AggregateFactory.create(name)
        for name in cmd.aggregator_args
    ]

    # Retrieve the records
    r_pool = workers.RetrievalPool(list(data_stores))
    # Filter records
    f_pool = workers.FilterPool(filters_objs)
    # Aggregate the result
    a_pool = workers.AggregatorPool(aggregator_objs)

    r_pool.join()
    f_pool.join()
    a_pool.join()

    log.log.info('For Args : {}, {}, {}'.format(
        cmd.logs_dir, cmd.filter_args, cmd.aggregator_args
    ))
    log.log.info('Final Results : {}'.format(a_pool.results))

    # write to cache
    cache_obj.set(cmd, a_pool.results)
    # output the result
    cmd.display_output(a_pool.results)


if __name__ == '__main__':
    main()
