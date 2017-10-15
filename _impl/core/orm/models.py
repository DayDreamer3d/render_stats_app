""" Module contains all the persistent store models.
"""
import itertools

import config


class Model(object):
    """ Base model class for persistent data model.
    """
    def __init__(self):
        self._name = None
        self._columns = []

    def __str__(self):
        return '{}({})'.format(
            self.__class__.__name__,
            self.columns[0].value
        )

    def __repr__(self):
        return self.__str__()

    @property
    def columns(self):
        """ All columns in of the model.
        """
        if not self._columns:
            raise ValueError('There are no columns defined for this model.')
        return self._columns

    def add_column(self, column):
        """ Add column to the model.

            Args:
                column (Column): column to be added in the model.
        """
        self._columns.append(column)

    def set_values(self, values):
        """ Set column values.

            Args:
                values (list) : add these values to model based on indices.
        """
        if len(values) != len(self.columns):
            raise ValueError("Length of values don't match to columns length.")
        for column, value in itertools.izip(self.columns, values):
            column.value = value


class Column(object):
    """ Model's column
    """

    def __init__(self, index, name, type_):
        """ Create a column.

            Args:
                index (int): index for the column
                name (str): name of the column.
                type_(type): supported type of value for the column.
        """
        self.index = index
        self.name = name
        self.type_ = type_
        self._value = None

    @property
    def value(self):
        """ Get the column's value
        """
        return self._value

    @value.setter
    def value(self, value):
        """ Set the column's value
        """
        if all([value, type(value) != self.type_]):
            value = self.type_(value)
        self._value = value


class RenderStats(Model):
    """ Render stats model.
    """
    def __init__(self):
        super(RenderStats, self).__init__()
        for index, (name, typ) in enumerate(config.Columns.all_):
            self.add_column(Column(index, name, typ))
