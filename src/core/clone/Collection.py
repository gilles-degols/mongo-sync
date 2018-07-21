from src.core.service.Mongo import Mongo
import time
import pymongo
from bson.objectid import ObjectId

class Collection:
    def __init__(self, configuration, db, coll):
        self.configuration = configuration
        self.db = db
        self.coll = coll
        self.mongo_primary = Mongo(configuration, is_primary=True)
        self.mongo_secondary = Mongo(configuration, is_primary=False)

        self.coll_stats = self.mongo_primary.collection_stats(db=self.db, coll=self.coll)
        self.previous_id = None

    """
        In charge of preparing the collection to synchronize, returns the various seeds we should use. 
    """
    def prepare_sync(self):
        average_object_size = self.coll_stats['avgObjSize']
        expected_documents  = self.coll_stats['count'] # It can increase but it's not a problem, it's only used for logging

        # Drop / Create the destination collection
        self.check_collection()

        # Get the various seeds
        seeds = self.list_seeds()
        if len(seeds) == 0:
            print("We should always have at least 2 seeds.")
            raise ValueError("Invalid seed number. Failure.")

        # Create and return the list of inputs necessary to create CollectionPart
        collection_parts = []
        previous_seed = seeds[0]
        for seed in seeds[1:]:
            collection_parts.append({
                'db': self.db,
                'coll': self.coll,
                'seed_start': previous_seed,
                'seed_end': seed,
                'total_seeds': len(seeds)
            })
            previous_seed = seed
        return collection_parts



    """
        Load the seeds we want to use for the synchronisation. Return a list of tuples, each tuple represent a range,
        the first value is the start of it, the second value, the end of it.
    """
    def list_seeds(self):
        # First, we need to be sure that we have an _id and if that's an objectid, otherwise we cannot use the same technique.
        # The oplog should only be tailed by one thread at a time, so we want to be sure to never create seeds for it.
        id_type = self.mongo_primary.id_type(self.db, self.coll)
        if id_type['has_id'] is False or id_type['is_object_id'] is False or (self.db == "local" and self.coll == "oplog.rs"):
            return [None, None]


        # Number of seeds we would like
        quantity = self.configuration.internal_maximum_seeds()
        if self.coll_stats['count'] <= 100*quantity: # Arbitrarily, we decide it's useless to use a lot of seeds if we only have a small number of documents
            return [({'_id':ObjectId('0'*24)},{'_id':ObjectId('f'*24)})]

        # Get various seeds
        seeds = self.mongo_primary.section_ids(self.db, self.coll, quantity=quantity)

        # TODO: In the future, if we want to be smart and allow retry of a failed sync, we should take the previous seeds
        # stored in the mongosync database, then make a simple query to see up to where they went

        # We order them to be able to return ranges. the ObjectId already allows to compare values.
        seeds = sorted(seeds, key=lambda seed: seed['_id'])

        # Always add the first and last seed
        seeds = [{'_id':ObjectId('0'*24)}] + seeds
        seeds.append({'_id':ObjectId('f'*24)})

        return seeds

    """
        Specific checks before writing to a collection
    """
    def check_collection(self):
        # Stats about the optional collection
        if self.db in self.mongo_secondary.list_databases() and self.coll in self.mongo_secondary.list_collections(self.db):
            destination_stats = self.mongo_secondary.collection_stats(db=self.db, coll=self.coll)
        else:
            destination_stats = {}

        # For internal db, we want to remove them by default, to avoid any problem (one exception: the oplog)
        if len(destination_stats) != 1 and self.db == 'local':
            if self.coll != 'oplog.rs' and False:
                # Not possible to drop every db
                self.mongo_secondary.drop(self.db, self.coll)
                self.destination_stats = {}

        # Optionally create a capped collection, but we only do that if it didn't exist before
        if self.coll_stats['capped'] is True and len(destination_stats) == 0:
            capped_max_size = self.coll_stats.get('maxSize', -1)
            capped_max = self.coll_stats.get('max', -1)
            if self.coll_stats['ns'] == 'local.oplog.rs':
                # Special case, we do not necessarily want to keep the same oplog size as the other node
                capped_max_size = self.configuration.mongo_oplog_size() * (1024 ** 3)

            if capped_max_size == -1:
                capped_max_size = None
            if capped_max == -1:
                capped_max = None

            self.mongo_secondary.create_collection(self.db, self.coll, capped=True, max=capped_max, max_size=capped_max_size)


    def __str__(self):
        return 'Collection:'+self.db+'.'+self.coll

    def __repr__(self):
        return self.__str__()
