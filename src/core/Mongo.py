#!/usr/lib/mongosync/environment/bin/python3.6
import errno
from math import floor, ceil
from errno import ENOENT, EDEADLOCK
import os
import signal
import subprocess
import time

from pymongo.errors import PyMongoError
from pymongo import MongoClient
from src.core.Configuration import Configuration
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
                    mongo.load_internal()
                # To easily see the queries done
                # print('Run mongo query: '+str(args)+' with '+str(kwargs))
                response = view_func(*args, **kwargs)
                return response
            except PyMongoError as e:
                dt = time.time() - st
                if dt >= mongo.configuration.mongo_access_attempt():
                    print('Problem to execute the query, maybe we are disconnected from MongoDB. ' +
                          'Max access attempt exceeded ('+str(int(dt))+'s >= '+str(mongo.configuration.mongo_access_attempt())+'). ' +
                          'Stop the mount.')
                    custom_kill(mongo.configuration)
                    # We want to exit the current loop
                    exit(1)
                else:
                    print('Problem to execute the query, maybe we are disconnected from MongoDB. Connect and try again.')
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
        Create an index
    """
    @retry_connection
    def create_index(self, db, coll, index):
        return self.instance[db][coll].create_index(index)

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
    def find(self, db, coll, query, projection=None):
        return self.instance[db][coll].find(query, projection, no_cursor_timeout=True)

    """
        A FindOneAndUpdate which always return the document after modification
    """
    @retry_connection
    def find_one_and_update(self, db, coll, query, update):
        result = self.instance[db][coll].find_one_and_update(query, update, return_document=ReturnDocument.AFTER)
        return result

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
        return self.instance[db][coll].insert_many(documents, ordered=False, bypass_document_validation=True)

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
        A simple delete_many
    """
    @retry_connection
    def delete_many(self, db, coll, query):
        return self.instance[db][coll].delete_many(query)

    """
        The drop command is only used for development normally
    """
    @retry_connection
    def drop(self, db, coll):
        return self.instance[db][coll].drop()