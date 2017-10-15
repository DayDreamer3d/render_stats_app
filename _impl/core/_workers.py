""" Module containing concurrency related code.
    Mainly type of wrokers and worker pools.

    Workers:
        work horses threads to the real work.

        RetrivalWorker - retrieval workers to fetch data from persistent store.
        FilterWorker - resposible for filteration of records.
        AggregatorWorker - aggregate the result.

    Worker Pool:
        pool of workers.

        WorkerPool - base worker pool class.
        RetrievalPool - pool to spawn RetrivalWorker workers.
        FilterPool - pool to spawn FilterWorker workers.
        AggregatorPool - pool to spawn AggregatorWorker workers.
"""

import collections
import Queue
import threading

import config
from _impl.core.compute import aggregators
from _impl.utils import log, utils


# TODO: why are these floating in air? should be in the class.
data_queue = Queue.Queue()
filter_queue = Queue.Queue()
result_queue = Queue.Queue()


def get_items(queue, prefetch_count=config.Concurrency.prefetch_count):
    """ Get items from the queue based on prefetch count.

        Args:
            queue (Queue): queue from which items will be fetched.

        Kwargs:
            pretch_count (int): number of items to be fetched at once.

        Retuens:
            tuple(list, bool): pair of items fetched and is queue empty.
    """
    records = []
    queue_empty = False
    for _ in range(prefetch_count):
        try:
            item = queue.get(timeout=config.Concurrency.timeout)
            queue.task_done()
            records.append(item)
        except Queue.Empty:
            queue_empty = True
            break
    return records, queue_empty


class WorkerPool(object):
    """ Base class for worker pool
    """
    def __init__(self, num_threads=0):
        """ Initialise the worker pool with given threads.

            Kwargs:
                num_threads(int): number of workers.
        """
        self.threads = []
        self.num_threads = num_threads

    def __str__(self):
        return '{}({})'.format(
            self.__class__.__name__, len(self.threads)
        )

    def join(self):
        """ Join all the workers.
        """
        for thread in self.threads:
            thread.join()


class RetrivalWorker(threading.Thread):
    """ Worker to retrieve data from persistent store.
    """
    def __init__(self, index, engines, output):
        """
            Args:
                engines (list): list of orm engines to rerieve the data.
                output (Queue): to put the data in post fetching.

        """
        super(RetrivalWorker, self).__init__()
        self.name = '{}_{}'.format(self.__class__.__name__, index)
        self.engines = engines
        self.output = output
        self.start()

    def run(self):
        """ Function to do the real work.
        """
        for engine in self.engines:
            records = engine.get_all_records()
            while True:
                try:
                    self.output.put(next(records))
                except StopIteration:
                    break


class RetrievalPool(WorkerPool):
    """ Pool for Retieval workers.
    """

    def __init__(self, engines, num_threads=config.Concurrency.retriever_threads):
        """ Initialise RetrievalPool workers.

            Args:
                engines (list): list of orm engines to rerieve the data.

            Kwargs:
                num_threads (int): number of workers.
        """
        super(RetrievalPool, self).__init__(num_threads)

        distribution = utils.distribution(len(engines), num_threads)
        for index, units in enumerate(distribution):
            items = [engines[index] for index in units]
            worker = RetrivalWorker(index, items, data_queue)
            self.threads.append(worker)

        log.log.info(str(self))

class FilterWorker(threading.Thread):
    """ Worker to filter the records.
    """
    def __init__(self, index, filters, input_, output):
        """
            Args:
                filters (list): type of filters to be applied on records.
                input_ (Queue): queue from the records will be fetched.
                output (Queue): queue on which filtered records will be kept.
        """
        super(FilterWorker, self).__init__()
        self.name = '{}_{}'.format(self.__class__.__name__, index)
        self.filters = filters
        self.input = input_
        self.output = output
        self.start()

    def run(self):
        """ Main worker method.
        """
        queue_empty = False
        while not queue_empty:
            records, queue_empty = get_items(self.input)

            if not records:
                return

            for filter_ in self.filters:
                records = list(filter_(records))
                if records:
                    log.log.info('{} -> Records({})'.format(filter_, len(records)))

            # put the filtered result onto the Queue
            # which will be consumed by the consumer
            for record in records:
                self.output.put(record)


class FilterPool(WorkerPool):
    """ Class to spawn FilterWorker type workers.
    """
    def __init__(self, filters, num_threads=config.Concurrency.filter_threads):
        """ Initialise FilterPool.

            Args:
                filters (list): list of filters.

            Kwargs:
                num_threads (int): number of workers.
        """
        super(FilterPool, self).__init__(num_threads)

        for index in range(num_threads):
            worker = FilterWorker(index, filters, data_queue, filter_queue)
            self.threads.append(worker)

        log.log.info(str(self))


class AggregatorWorker(threading.Thread):
    def __init__(self, index, aggregator_objs, input_, output):
        """
            Args:
                aggregator_objs (list): list of aggregator types.
                input_ (Queue): queue from the records will be fetched.
                output (Queue): queue on which filtered records will be kept.
        """
        super(AggregatorWorker, self).__init__()

        self.name = '{}_{}'.format(self.__class__.__name__, index)

        self.aggregator_objs = aggregator_objs
        self.input = input_
        self.output = output
        self.start()

    def run(self):
        """ Main worker method.
        """
        initial_result = {}
        queue_empty = False

        while not queue_empty:
            records, queue_empty = get_items(self.input)
            if not records:
                break

            # get the fist stage result for all aggregators
            for aggregator in self.aggregator_objs:
                records_result = initial_result.get(aggregator)
                records_result = aggregator.record_aggregation(records, records_result)
                initial_result[aggregator] = records_result
                log.log.info('Initial Result for {} -> Records({})'.format(aggregator, initial_result[aggregator]))

        # store the result in Queue
        self.output.put(initial_result)
        log.log.debug('Queue({}) | Initial Dict -> {}'.format(
                    self.output.qsize(), initial_result)
        )


class AggregatorPool(WorkerPool):
    """ Class resposible for spawning AggregatorWorker type workers.
    """

    def __init__(self, aggregator_objs, num_threads=config.Concurrency.aggregator_threads):
        """ Initialise AggregatorPool.

            Args:
                aggregator_objs (list): list of aggregator types.

            Kwargs:
                num_threads (int): number of workers.
        """
        super(AggregatorPool, self).__init__(num_threads)

        self.aggregator_objs = aggregator_objs
        self.results = []

        self.input = filter_queue
        self.output = result_queue

        for index in range(num_threads):
            worker = AggregatorWorker(index, aggregator_objs, self.input, self.output)
            self.threads.append(worker)

        log.log.info(str(self))

    def join(self):
        """ Join the threads.
        """
        super(AggregatorPool, self).join()
        self.finalise()

    def finalise(self):
        """ Finalise the result by aggregating the results from all the workers.
        """
        # Deque items and store in a dict, in such a way
        # such that, {aggregator: [result from workers]}

        initial_results = collections.defaultdict(list)
        while not self.output.empty():
            worker_result = self.output.get()
            self.output.task_done()
            for aggregator, result in worker_result.iteritems():
                initial_results[aggregator].append(result)

        log.log.debug('initial_results -> {}'.format(initial_results))

        # To preserve the ordering of final result
        # loop through the affregators
        # and get the final result
        for aggregator in self.aggregator_objs:
            for a, r in initial_results.iteritems():
                if a is aggregator:
                    result = aggregator.result_aggregation(r)
                    break

            log.log.info('Final Result for {} -> Result({})'.format(aggregator, result))
            self.results.append(result)
