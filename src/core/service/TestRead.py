import random
import time
import pymongo
from src.core.service.Mongo import Mongo

class TestRead:

    def __init__(self, configuration):
        self.configuration = configuration
        self.db = self.configuration.internal_database()
        self.coll = self.configuration.internal_test_write_collection()

        self.primary = Mongo(self.configuration, is_primary=True)
        self.secondary = Mongo(self.configuration, is_primary=False)

    """
        Small method to loads GBs of data as fast as possible in a mongodb instance, to test the mongosync speed afterwards
    """
    def start(self):
        print('Reading data from the mongosync database.')
        st = time.time()
        n = 0
        cursor = self.primary.find(db=self.db, coll=self.coll, query={}, sort_field='_id', sort_order=pymongo.ASCENDING)
        for doc in cursor:
            n += 1
        dt = time.time() - st
        print('Read ' + str(n) + ' documents in ' + str(int(dt)) + 's ('+str(int(n/dt))+' docs/s).')