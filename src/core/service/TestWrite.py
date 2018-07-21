import random
import time
from src.core.service.Mongo import Mongo

class TestWrite:

    def __init__(self, configuration):
        self.configuration = configuration
        self.db = self.configuration.internal_database()
        self.coll = self.configuration.internal_test_write_collection()

        self.coll_expected_size = self.configuration.internal_test_write_size()
        self.document_size = self.configuration.internal_test_write_document_size()

        self.string_seed = TestWrite.generate_string_seed(50*int(max(1024,self.document_size)))

        self.mongo = Mongo(self.configuration, is_primary=False)

    """
        Small method to loads GBs of data as fast as possible in a mongodb instance, to test the mongosync speed afterwards
    """
    def start(self):
        print('Inserting data in the database, we want to go up to '+str(self.coll_expected_size)+'GB.')
        self.mongo.drop(self.db, self.coll)
        current_size = 0

        # To avoid insane number of docs inserted at once, we estimate how much we can have at the same time
        docs_per_insert = int(16 * 1024 * 1024 / self.document_size)
        n = 0
        i = 0
        st = time.time()
        dt = 0
        while current_size < self.coll_expected_size * (1024 ** 3):
            docs = [{"stuff":"hello","raw":self.random_string_from_seed(self.document_size)} for i in range(docs_per_insert)]
            self.mongo.insert_many(self.db, self.coll, docs)
            n += len(docs)
            i += 1

            if i % 10 == 0:
                raw_stats = self.mongo.collection_stats(self.db, self.coll)
                current_size = raw_stats.get('storageSize', 0)
                if current_size == 0:
                    print("Warning: We got 0 bytes as storage size for the TestWrite. Did you delete the collection during the process? We'll handle it anyway.")

                dt = time.time() - st
                print('Inserted '+str(n)+' documents in '+str(int(dt))+'s. Current size is '+str(int(current_size/(1024 ** 3)))+'/'+str(self.coll_expected_size)+'GB.')

        print('Inserted ' + str(n) + ' documents in ' + str(int(dt)) + 's. Current size is ' + str(int(current_size / (1024 ** 3))) + '/' + str(self.coll_expected_size) + 'GB.')
        print('The end!')

    """
        To avoid the compression of MongoDB to mess with our test, we create a random string used as seed for all document.
        This is a "slow" process so we cannot generate a new random string for each document  
    """
    @staticmethod
    def generate_string_seed(size):
        print('Generate random string seed, it might take some time...')
        letters = 'azertyuiopqsdfghjklmwxcvbnAZERTYUIOPQSDFGHJKLMWXCVBN0123456789'
        return ''.join([random.choice(letters) for i in range(size)])

    """
        Return a "random" list of characters from the seed string 
    """
    def random_string_from_seed(self, size):
        # In fact, really not random, but whatever, we'll hope MongoDB will not be able to compress the data too efficiently
        start = random.randint(0,len(self.string_seed) - size)
        return self.string_seed[start:start+size]
