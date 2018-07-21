
import time
from src.core.Mongo import Mongo

class CollectionPart:

    """
        Seeds can be None if there is no {"_id": ObjectId()} in the document database, in that case there will be only one
        thread in charge of copying the database
    """
    def __init__(self, configuration, db, coll, seed_start=None, seed_end=None, total_seeds=1):
        self.configuration = configuration
        self.db = db
        self.coll = coll
        if seed_start is None or seed_end is None: # It ease the code below
            seed_start = {'_id':None}
            seed_end = {'_id':None}
        self.seed_start = seed_start
        self.seed_end = seed_end
        self.total_seeds = total_seeds # Total number of seeds, which can be seen as the number of instances of CollectionPart
        self.mongo_primary = Mongo(configuration, is_primary=True)
        self.mongo_secondary = Mongo(configuration, is_primary=False)

        self.coll_stats = self.mongo_primary.collection_stats(db=self.db, coll=self.coll)
        self.previous_id = None

    """
        In charge of syncing the entire part of the collection assigned to it, so every document between two given
        seeds. The collection must be initially created by the Collection class, this is not the job of this class.
    """
    def sync(self):
        average_object_size = self.coll_stats['avgObjSize']
        expected_documents  = int(self.coll_stats['count'] / self.total_seeds) # It can increase but it's not a problem, it's only used for logging

        # Write limit is 16MB, so we put a security factor by only using ~12 MB
        limit_write = int(12 * (1024 ** 2) / average_object_size)
        # For the read-limit, we can arbitrarily takes up to 16 MB * 10, to avoid using too much RAM.
        limit_read = int(limit_write * 10)

        # Raw estimation of the data size for the current collection part
        storage_size_part = self.coll_stats['storageSize']/((1024**3) * self.total_seeds)

        objects_in_it = True
        offset = 0
        st = time.time()
        read_time = 0
        write_time = 0
        i = 0
        print(str(self)+' (start-sync): '+str(expected_documents)+' docs, ~'+str(int(storage_size_part))+'GB.')
        while objects_in_it:
            raw_stats = self.sync_section(offset, limit_read, limit_write)
            offset += raw_stats['quantity']
            read_time += raw_stats['read_time']
            write_time += raw_stats['write_time']

            if raw_stats['quantity'] < limit_read:
                objects_in_it = False

            i += 1
            if i % 50 == 0:
                if offset >= expected_documents:
                    # To have better logs, we check the remaining entries
                    self.coll_stats = self.mongo_primary.collection_stats(db=self.db, coll=self.coll)
                    expected_documents = int(self.coll_stats['count'] / self.total_seeds)

                ratio = int(1000 * offset / expected_documents)/10 # To have the format 100.0%
                dt = time.time() - st
                average_speed = 1
                expected_remaining_time = 0
                if dt >= 0 and offset / dt > 0:
                    average_speed = offset / dt
                    expected_remaining_time = int((expected_documents - offset) / (average_speed * 60)) # In minutes

                time_log = 'Read time: '+str(int(100*read_time/dt))+'%, write time: '+str(int(100*write_time/dt))+'%'
                print(str(self)+' (syncing): '+str(offset)+'/'+str(expected_documents)+' docs ('+str(ratio)+'%, '+str(int(average_speed))+' docs/s). Remaining time: ~'+str(expected_remaining_time)+' minutes. '+time_log)

        dt = time.time() - st
        print(str(self)+' (end-sync): '+str(offset)+' docs, '+str(int(storage_size_part))+'GB. Time spent: '+str(int(dt))+'s.')

        # We return some stats
        return {'quantity':offset,'read_time':read_time,'write_time':write_time}


    """
        In charge of syncing its part of the collection (between the two given seeds). Return the number of synced objects.
        We are not using any iterator in this case, so the method should normally not crash if the connexion is lost
        at the wrong time.
    """
    def sync_section(self, offset, limit_read, limit_write):
        # Fetching objects to sync from the primary. Not all documents have a "_id" field, so be careful. Performance will be very
        # slow if you do not have it, and it might crash if there are too many documents without any "_id" (TODO we should switch
        # to cursors in that case). We assume that if a collection contains a document with "_id", all documents have it.
        st = time.time()
        query = {
            '_id':{
                '$gte': self.seed_start['_id'],
                '$lte': self.seed_end['_id'] # To be sure the final item, it's better to do one extra insert each time
            }
        }
        skip = 0
        if self.previous_id is not None:
            query['_id']['$gte'] = self.previous_id

        if self.seed_start['_id'] is None or self.seed_end['_id'] is None:
            del query['_id']['$lte']
            if self.previous_id is None:
                # Special code to handle collection without any _id at all, or at the first iteration
                query = {}
                skip = offset

        objects = list(self.mongo_primary.find(self.db, self.coll, query=query, skip=skip, limit=limit_read, sort_field='_id'))
        read_time = time.time() - st

        # Writing the objects to the secondary
        st = time.time()
        for i in range(0, len(objects), limit_write):
            try:
                self.mongo_secondary.insert_many(self.db, self.coll, objects[i:i + limit_write])
            except Exception as e:
                print(
                    'Exception while trying to insert ' + str(len(objects[i:i + limit_write])) + ' documents in ' + str(
                        self) + ' (' + str(e) + '). Try once again, but one document after another.')
                # Maybe we exceeded the 16MB, so better insert every document separately
                for j in range(i, i + limit_write):
                    self.mongo_secondary.insert_many(self.db, self.coll, [objects[j]])
        write_time = time.time() - st

        # Now we can assume that we correctly inserted the expected documents, so we can store the new start for the section
        # to copy
        if len(objects) >= 1 and '_id' in objects[-1]:
            self.previous_id = objects[-1]['_id']

        return {'quantity': len(objects), 'read_time': read_time, 'write_time': write_time}




    def __str__(self):
        return 'CollectionPart:' + self.db + '.' + self.coll+':['+str(self.seed_start['_id'])+';'+str(self.seed_end['_id'])+']'

    def __repr__(self):
        return self.__str__()