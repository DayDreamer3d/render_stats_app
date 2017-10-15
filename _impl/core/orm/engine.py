""" Engine for persistent store interactions.
"""
import csv


# May be there could be an interface to choose from different persistent stores.

class Engine(object):
    """ Class to create the engine.
    """
    def __init__(self, store_path, model):
        """ Initialise the class.

            Args:
                store_path (str): path to data store.
                model (Model): correspoding object model.
        """
        self.store_path = store_path
        self.model = model

    def get_all_records(self):
        """ Get all the records from the data store.

            Yield:
                Model: the model per record.
        """
        with open(self.store_path, 'r') as file_handler:
            records = csv.reader(file_handler)

            for record in records:
                model = self.model()
                model.set_values(record)
                yield model
