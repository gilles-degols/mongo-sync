#!/usr/lib/mongosync/environment/bin/python3.6
import json

"""
    Every configuration variable must be accessed through a method.
"""
class Configuration:
    # This value can be overrided by the user
    FILEPATH = '/etc/mongosync/mongosync.json'

    def __init__(self):
        self.load(filepath=Configuration.FILEPATH)

    """
        Read application
    """
    def load(self, filepath):
        with open(filepath, 'r') as f:
            self.conf = json.load(f)

    """
        Return the mongo host out of synchronisation which would like to synchronize
    """
    def mongo_host_out_of_sync(self):
        return self.conf['mongo']['host']['out_of_sync']

    """
        Return the mongo host which will be the basis for the synchronisation
    """
    def mongo_host_in_sync(self):
        return self.conf['mongo']['host']['in_sync']

    """
        To allow a long synchronisation without crash, we might need to set a big number for the oplog
    """
    def mongo_oplog_size(self):
        return self.conf['mongo']['oplog_size_GB']

    """
        Maximum number of seconds we will try to reconnect to MongoDB if we lost the connection at inappropriate time.
        Value <= 0 means infinity.
    """
    def mongo_access_attempt(self):
        if self.conf['mongo']['access_attempt_s'] <= 0:
            # Kinda infinite
            return 3600*24*365*100
        return self.conf['mongo']['access_attempt_s']

    """
        Write concern to write to MongoDB. 0 disable write acknowledgement
        Value <= 0 means infinity.
    """
    def mongo_write_acknowledgement(self):
        return self.conf['mongo']['write_acknowledgement']

    """
        If set to True, wait for the MongoDB journaling to acknowledge the write
        Value <= 0 means infinity.
    """
    def mongo_write_j(self):
        return self.conf['mongo']['write_j']

    """
        The database to use for the command (it will use the "in-sync" node obviously)
    """
    def internal_database(self):
        return self.conf['internal']['database']

    """
        The maximum number of seeds we want to have for any collection. For small collection we can arbitrarily decide
        to reduce their number.
    """
    def internal_maximum_seeds(self):
        return self.conf['internal']['maximum_seeds']

    """
        The collection to use to write a lot of data for performance test
    """
    def internal_test_write_collection(self):
        return self.conf['internal']['test_write_collection']

    """
        The number of data we want to write to the TestWrite collection. Return a number in GB.
    """
    def internal_test_write_size(self):
        return self.conf['internal']['test_write_size_GB']

    """
        The average document size for the test-write collection. Return a number in bytes.
    """
    def internal_test_write_document_size(self):
        return self.conf['internal']['test_write_document_bytes']

    """
        Indicates if we are in a development mode (= clean database before mount for example) or not.
    """
    def is_development(self):
        return self.conf['development']
