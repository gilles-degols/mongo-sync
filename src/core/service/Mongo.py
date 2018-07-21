#!/usr/lib/mongosync/environment/bin/python3.6
import errno
from math import floor, ceil
from errno import ENOENT, EDEADLOCK
import os
import signal
import subprocess
import time
from datetime import datetime, timezone
from bson.objectid import ObjectId
import pymongo
from pymongo.errors import PyMongoError
from pymongo import MongoClient
from pymongo.collection import ReturnDocument

from functools import wraps
import copy

"""
    Custom decorator to easily handle a MongoDB disconnection.
    We can (should) even use the wrapper in the connect / load method 
"""
def retry_connection(view_func):
    def custom_kill(config):
        command = 'pkill -f 9 /usr/bin/mongosync'
        print('Try to kill the current process with all related threads: ' + str(command))
        # Run in background
        subprocess.run([command], shell=True, stdout=subprocess.PIPE)

        # We wait a bit, as we hope the umount will work.
        time.sleep(15)

        print('It seems the pkill did not work, kill the process itself.')
        SIGKILL = 9
        os.kill(os.getpid(), SIGKILL)


    def _decorator(*args, **kwargs):
        new_connection_attempt = False
        st = time.time()
        mongo = args[0]
        while True:
            try:
                if new_connection_attempt is True:
                    time.sleep(0.5)
                    mongo.connect()
                # To easily see the queries done
                # print('Run mongo query: '+str(args)+' with '+str(kwargs))
                response = view_func(*args, **kwargs)
                return response
            except PyMongoError as e:
                dt = time.time() - st
                if dt >= mongo.configuration.mongo_access_attempt():
                    print('Problem to execute the query ('+str(e)+'), maybe we are disconnected from MongoDB. ' +
                          'Max access attempt exceeded ('+str(int(dt))+'s >= '+str(mongo.configuration.mongo_access_attempt())+'). ' +
                          'Stop the mount.')
                    custom_kill(mongo.configuration)
                    # We want to exit the current loop
                    exit(1)
                else:
                    print('Problem to execute the query ('+str(e)+'), maybe we are disconnected from MongoDB. Connect and try again.')
                    new_connection_attempt = True

    return wraps(view_func)(_decorator)

"""
    This class will implement every method that we need to connect to MongoDB, and every query should be run through it (with the exceptions of tests). 
    This is also an easy to handle the disconnection to MongoDB during a short amount of time.
"""
class Mongo:
    def __init__(self, configuration, is_primary):
        self.is_primary = is_primary  # Correct value is "True" or "False"
        self.configuration = configuration

        retry_connection(self.connect())

    """
        Establish a connection to mongodb
    """
    def connect(self):
        host = self.configuration.mongo_host_in_sync()
        if self.is_primary is False:
            host = self.configuration.mongo_host_out_of_sync()
        mongo_path = 'mongodb://' + host
        self.instance = MongoClient(mongo_path, w=self.configuration.mongo_write_acknowledgement(), j=self.configuration.mongo_write_j())

    """
        Create a collection, useful for a cappped collection
    """
    @retry_connection
    def create_collection(self, db, coll, capped=False, max=None, max_size=None):
        database = self.instance[db]
        return database.create_collection(coll, capped=capped, max=max, size=max_size)

    """
        Create an index
    """
    @retry_connection
    def create_index(self, db, coll, options):
        return self.instance[db][coll].create_index(**options)

    """
        Simply retrieve any document
    """
    @retry_connection
    def find_one(self, db, coll, query):
        return self.instance[db][coll].find_one(query)

    """
        A generic find function, which might be problematic to handle if we get a connection error while iterating on it.
        It needs to be handle on the caller side to avoid any problem.
    """
    @retry_connection
    def find(self, db, coll, query, skip, limit, projection=None, sort_field = '_id', sort_order=pymongo.ASCENDING):
        return self.instance[db][coll].find(query, projection, no_cursor_timeout=True).skip(skip).limit(limit).sort(sort_field, sort_order)

    """
        A specific find method to read the oplog with a tailable cursor
    """
    @retry_connection
    def find_oplog(self, query, skip, limit, projection=None):
        if len(query) == 0:
            # If no query given, we would be automatically put at the end of the oplog, but we want to start from the begging
            try:
                first = self.instance["local"]["oplog.rs"].find().sort('$natural', pymongo.ASCENDING).limit(-1).next()
                query = {'ts':{'$gt':first['ts']}}
            except Exception as e:
                print('Problem while fetching the first element of the oplog: '+str(e)+'. We start from the end instead.')
                query = {}

        return self.instance["local"]["oplog.rs"].find(query, projection, no_cursor_timeout=True, cursor_type=pymongo.CursorType.TAILABLE_AWAIT, oplog_replay=True).skip(skip).limit(limit)

    """
        A FindOneAndUpdate which always return the document after modification
    """
    @retry_connection
    def find_one_and_update(self, db, coll, query, update):
        result = self.instance[db][coll].find_one_and_update(query, update, return_document=ReturnDocument.AFTER)
        return result

    """
        A list of ids from a given collection, which could be use to "equally" split the collection. Return an empty list if not possible to do it.
        Duplicate seeds are possible. _ids are not returned in any specific order.
    """
    def section_ids(self, db, coll, quantity):
        # The "$sample" is slow, so we will try to generate random object ids ourselves

        # We want to have some information about the smallest and biggest _id (we suppose that the _id is monotonously increasing)
        first = list(self.find(db=db, coll=coll, query={}, skip=0, limit=1, projection=None, sort_field='_id', sort_order=pymongo.ASCENDING))
        if len(first) == 0:
            return []
        last = list(self.find(db=db, coll=coll, query={}, skip=0, limit=1, projection=None, sort_field='_id', sort_order=pymongo.DESCENDING))
        if len(last) == 0:
            return []
        first = first[0]
        last = last[0]

        # Arbitrarily generate object ids between the minimal/maximal values
        first_timestamp = first['_id'].generation_time.replace(tzinfo=timezone.utc).timestamp()
        last_timestamp = last['_id'].generation_time.replace(tzinfo=timezone.utc).timestamp()
        step = int(max(1,(last_timestamp - first_timestamp) / quantity))
        section_ids = []
        for offset in range(int(first_timestamp), int(last_timestamp), step):
            current_date = datetime.utcfromtimestamp(offset)
            section_id = {'_id':ObjectId.from_datetime(current_date)}
            section_ids.append(section_id)

        return section_ids

    """
        Indicates if the collection contains at least one document with an "_id" field, and if it's an ObjectId 
    """
    def id_type(self, db, coll):
        first = list(self.find(db=db, coll=coll, query={}, skip=0, limit=1, projection=None, sort_field='_id', sort_order=pymongo.ASCENDING))
        if len(first) == 0:
            return {'has_id':False,'is_object_id':False}
        first = first[0]

        has_id = '_id' in first
        is_object_id = False
        if has_id:
            is_object_id = isinstance(first['_id'], ObjectId)
        return {'has_id':has_id,'is_object_id':is_object_id}

    """
        A simple insert_one
    """
    @retry_connection
    def insert_one(self, db, coll, document):
        return self.instance[db][coll].insert_one(document)

    """
        A simple insert_many
    """
    @retry_connection
    def insert_many(self, db, coll, documents):
        try:
            result = self.instance[db][coll].insert_many(documents, ordered=False, bypass_document_validation=True)
        except pymongo.errors.BulkWriteError as e:
            # We don't want to crash on duplicate key errors
            for err in e.details.get('writeErrors', []):
                if err['code'] != 11000:
                    print(e.details)
                    raise e
            result = []
        return result

    """
        Stats on a given collection
    """
    @retry_connection
    def collection_stats(self, db, coll):
        # For whatever reason you have to go through an intermediate variable to run the command
        database = self.instance[db]
        try:
            result = database.command("collstats", coll)
        except pymongo.errors.OperationFailure as e:
            print('Problem to get stats for '+str(db)+'.'+str(coll)+'. Generally it is because of the collection not existing.')
            result = {}

        return result

    """
        Information about each index on a collection
        {
            <index_id> : {
                'key': [
                    (<field_name>, <order>)
                ]
                <optional_elements>
            }
        }
    """
    @retry_connection
    def get_indexes(self, db, coll):
        return self.instance[db][coll].index_information()


    """ 
        A simple delete_many
    """
    @retry_connection
    def delete_many(self, db, coll, query):
        return self.instance[db][coll].delete_many(query)

    """
        List all databases
    """
    def list_databases(self):
        return self.instance.database_names()

    """
        List all collections from a database
    """
    def list_collections(self, db):
        return self.instance[db].list_collection_names()

    """
        The drop command is only used for development normally
    """
    @retry_connection
    def drop(self, db, coll):
        return self.instance[db][coll].drop()