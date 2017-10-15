""" Module containing all the aggregators for the app, they are responsible
    for aggregating the result post filtering.
    AggregateFactory class produces the asked aggregators.
    Type of aggregators are:
        SuccessCount - count the number of successful renders.
        AverageTime - aggregate average elapsed time took for renders.
        AverageCpu - average cpu used for renders.
        AverageRam - average memory consumed for renders.
        MaximumRam - peak memory availed by renders.
        MaximumCpu - maximum cpu used by renders.
"""
import config


class Aggregator(object):
    """ Aggregator base class.
    """
    def __init__(self, aggregator_column):
        """ Initialise the aggregator column in here.
        """
        self.aggregator_column = aggregator_column

    def __str__(self):
        return 'Aggregator({})'.format(self.__class__.__name__)

    def __repr__(self):
        return str(self)

    def record_aggregation(self, recrods, result):
        """ Method to perform aggregation on filtered records.
            Should be overriden in subclasses.
        """
        pass

    def result_aggregation(self, results):
        """ Aggregation on results obtained from record_aggregation.
            Could be overriden in subclasses.
        """
        pass


class SuccessCount(Aggregator):
    """ Success count aggregator class.
        Responsible for counting the number of successful renders.
    """
    def __init__(self):
        super(SuccessCount, self).__init__(config.Columns.success)

    def record_aggregation(self, records, result):
        """ count the successful records
        """
        result = result or 0
        return result + len(records)

    def result_aggregation(self, results):
        """ sum all the counts obtained in record_aggregation.
        """
        return sum(results)



class AverageAggregator(Aggregator):
    """ Class to find the average properties of renders.
    """

    def record_aggregation(self, records, result):
        """ Sum the values from provided records.

            Args:
                records (list): List of filtered recrods
                result (tuple): Pair of sum of records and length of records

            Returns:
                tuple: Pair of sum of records and length of records.
        """
        result = result or (0, 0)
        values = []
        for record in records:
            for column in record.columns:
                if column.name == self.aggregator_column:
                    values.append(column.value)
                    break
        result = (sum(values + [result[0]]), len(values) + result[1])
        return result

    def result_aggregation(self, results):
        """ Average out all the results got from workers.

            Args:
                results (list): List of worker results each containing a sum and
                                length of records.

            Returns:
                float: average property of renders.
        """
        # it might look bad to perform two loops.
        # but our prefetch count is 10 (default and advised) i.e. max 10 items at a time
        # this will happen in no time.
        total_sum = [result[0] for result in results]
        total_records = [result[1] for result in results]
        return sum(total_sum) / sum(total_records)


class AverageTime(AverageAggregator):
    """ Class to find the average elapsed time for renders.
    """
    def __init__(self):
        super(AverageTime, self).__init__(config.Columns.elapsed_time)


class AverageRam(AverageAggregator):
    """ Class to aggregate the average ram used for renders.
    """
    def __init__(self):
        super(AverageRam, self).__init__(config.Columns.maxram)


class AverageCpu(AverageAggregator):
    """ Class to calculate the average cpu.
    """
    def __init__(self):
        super(AverageCpu, self).__init__(config.Columns.maxcpu)


class MaximumRam(Aggregator):
    """ Caclulate the maximum ram usage by renders.
    """
    def __init__(self):
        super(MaximumRam, self).__init__(config.Columns.maxram)

    def record_aggregation(self, records, max_value):
        """ Find the max ram value from provided records.
        """
        max_value = max_value or 0
        values = []
        for record in records:
            for column in record.columns:
                if column.name == self.aggregator_column:
                    values.append(column.value)
                    break
        return max(values + [max_value])

    def result_aggregation(self, results):
        """ Finally, aggregate uppon the results obtained by
            finding the max ram value.
        """
        return max(results)


class MaximumCpu(Aggregator):
    """ Class to aggregate over records and get the maximum cpu usage.
    """
    def __init__(self):
        super(MaximumCpu, self).__init__(config.Columns.maxcpu)

    def record_aggregation(self, records, max_value):
        """ Aggregate over the records and find the max. cpu value.
        """
        max_value = max_value or 0
        values = []
        for record in records:
            for column in record.columns:
                if column.name == self.aggregator_column:
                    values.append(column.value)
                    break
        return max(values + [max_value])

    def result_aggregation(self, results):
        """ Find the max of all the results.
            That would be considered our final result.
        """
        return max(results)


class AggregateFactory(object):
    """ Factory class to create different type of aggregators.
    """

    @classmethod
    def create(cls, typ):
        """ Method creates different Aggreagator types.
        """
        return {
            'avgram': AverageRam,
            'avgcpu': AverageCpu,
            'avgtime': AverageTime,
            'success': SuccessCount,
            'maxram': MaximumRam,
            'maxcpu': MaximumCpu,
        }.get(typ)()
