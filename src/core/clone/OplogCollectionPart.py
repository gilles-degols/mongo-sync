
import time
from src.core.clone.CollectionPart import CollectionPart
import pymongo

"""
    The oplog collection is a bit different than the others as there is no _id, and we want 
"""
class OplogCollectionPart(CollectionPart):
    def __init__(self, *args, **kwargs):
        CollectionPart.__init__(self, *args, **kwargs)

        if self.seed_start['_id'] is not None or self.seed_end['_id'] is not None:
            raise ValueError("There should be only one OplogCollectionPart!")

    def continue_fetching(self, received_quantity, expected_quantity):
        # There is no end to the fetching phase of the oplog. The only way to stop it, it's when the user manually ctrl+c
        # the process to remove it from maintenance.
        return True

    def sync_section(self, offset, limit_read, limit_write):
        # Fetching objects to sync from the primary. The oplog does not have any _id, nor index, so we need to use a cursor
        # as much as possible. But apparently they implemented some optimisations in the oplog to query the "ts" field fast even
        # without index.
        st = time.time()
        query = {}
        if self.previous_id is not None: # previous_id is the "ts" field in this case.
            query['ts']['$gt'] = self.previous_id

        cursor = self.mongo_primary.find_oplog(query=query, skip=0, limit=limit_read)
        read_time = time.time() - st

        # Writing the objects to the secondary
        st = time.time()
        subset = []
        n = 0
        while cursor.alive:
            for doc in cursor:
                subset.append(doc)
                n += 1
                if len(subset) >= limit_write:
                    self.insert_subset(subset)
                    self.previous_id = doc['ts']
                    subset = []

                    # To replicate the expected behavior of this method versus the CollectionPart implementation, we
                    # stop reading and wait for the next call
                    break

            if n >= limit_write:
                break
            else:
                # If there is no new document for 1 second, the iteration on the cursor stop, so we wait a bit before trying again
                if len(subset) >= 1:
                    self.insert_subset(subset)
                    self.previous_id = subset[-1]['ts']
                    subset = []
            time.sleep(1)

        if len(subset) >= 1:
            self.insert_subset(subset)
        write_time = time.time() - st

        return {'quantity': n, 'read_time': read_time, 'write_time': write_time}

    def __str__(self):
        return 'OplogCollectionPart:' + self.db + '.' + self.coll+':['+str(self.seed_start['_id'])+';'+str(self.seed_end['_id'])+']'

    def __repr__(self):
        return self.__str__()