""" Module containing all the filters for the app, they are responsible
    for filtering out the undesired render records and keep the required ones.
    FilterFactory class is resposible for creating the needed fitlers.
    Type of filters are:
        AppFilter - Filter the renders based on app name provided
        RendererFilter - Filter the renders based on renderer name provided
        SuccessFilter - Include the failed renders.
"""
import config


class Filter(object):
    """ Base class for all the filters.
    """

    def __init__(self, arg_value, filter_column):
        """ Initialse the values.

        Args:
            arg_value (str) - user provided filter value (e.g maya).
            filter_column (str) - which column should be considered (e.g. app).
        """
        self.arg_value = arg_value
        self.filter_column = filter_column

    def __str__(self):
        return 'Filter({})'.format(self.__class__.__name__)

    def __repr__(self):
        return str(self)

    def filter_func(self, record):
        """ Function containing the filter logic.
            Could be overridden in sub-classes.

            Args:
                record (RenderStats) - single render stat. object.

            Return:
                bool : provided values matches to the current column value.
        """
        for column in record.columns:
            if column.name == self.filter_column:
                return self.arg_value == column.value

    def __call__(self, records):
        """ Invoke this method when fitler instance is called.

            Args:
                records (list) - containing RenderStats objects to be filtered.

            Returns:
                FilterObject : iterator containing desired records.
        """
        return filter(self.filter_func, records)


class AppFilter(Filter):
    """ Class for filtering provided records based on aap.
    """
    def __init__(self, arg_value):
        super(AppFilter, self).__init__(arg_value, config.Columns.app)


class RendererFilter(Filter):
    """ Class for filtering provided records based on renderer.
    """
    def __init__(self, arg_value):
        super(RendererFilter, self).__init__(arg_value, config.Columns.renderer)


class SuccessFilter(Filter):
    """ Class for filtering provided records based on failed renders.
    """
    def __init__(self, arg_value):
        super(SuccessFilter, self).__init__(arg_value, config.Columns.success)


class FilterFactory(object):
    """ Factory class to create different type of Filters.
    """

    @classmethod
    def create(cls, typ, value):
        """ Method creates different Filter types.

            Args:
                typ (str) - type of filter.
                value (str) - user defined value for the argument.

            Returns:
                Filter: Filter object.
        """
        return {
            config.Columns.app: AppFilter,
            config.Columns.renderer: RendererFilter,
            config.Columns.success: SuccessFilter,
        }.get(typ)(value)
