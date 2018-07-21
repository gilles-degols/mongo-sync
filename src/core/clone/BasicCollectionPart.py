
import time
from src.core.clone.CollectionPart import CollectionPart

class BasicCollectionPart(CollectionPart):

    def continue_fetching(self, received_quantity, expected_quantity):
        return received_quantity >= expected_quantity

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
            self.insert_subset(objects[i:i + limit_write])
        write_time = time.time() - st

        # Now we can assume that we correctly inserted the expected documents, so we can store the new start for the section
        # to copy
        if len(objects) >= 1 and '_id' in objects[-1]:
            self.previous_id = objects[-1]['_id']

        return {'quantity': len(objects), 'read_time': read_time, 'write_time': write_time}

    def __str__(self):
        return 'BasicCollectionPart:' + self.db + '.' + self.coll+':['+str(self.seed_start['_id'])+';'+str(self.seed_end['_id'])+']'

    def __repr__(self):
        return self.__str__()