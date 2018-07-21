from src.core.service.Mongo import Mongo
from src.core.service.Configuration import Configuration
from src.core.clone.Collection import Collection
from src.core.clone.BasicCollectionPart import BasicCollectionPart
from src.core.clone.OplogCollectionPart import OplogCollectionPart

import multiprocessing as mp
from queue import Empty as QueueEmpty

"""
    Function (in another thread than the main program) to handle the clone of any CollectionPart
"""
def clone_collection_part(qi, qo, job_id, common_info):
    # Create the history for a specific pharmacy
    print('Process '+str(job_id)+': start to clone CollectionParts.')

    Configuration.FILEPATH = common_info['configuration_filepath']
    configuration = Configuration()
    total = qi.qsize() # Only use that information for logging
    while True:
        try:
            data = qi.get(timeout=1)  # Timeout after 1 second, no need to wait more than that
            if data == 'DONE':
                print('Process ' + str(job_id) + ': job done, stop here this process.')
                qo.put('DONE')
                return
            else:
                if data['collection_part']['db'] == "local" and data['collection_part']['coll'] == 'oplog.rs':
                    print('Process ' + str(job_id) + ': Start long-running job to clone the oplog')
                else:
                    print('Process '+str(job_id)+': Start CollectionParts ~'+str(total - qi.qsize())+'/'+str(total))

                data['collection_part']['configuration'] = configuration
                collection_part = Core.create_collection_part(inputs = data['collection_part'])
                collection_part.sync()

        except QueueEmpty:
            qo.put('DONE')
            return  # Exit when all work is done
        except:
            raise  # Raise all other errors


class Core:

    def __init__(self, configuration):
        self.configuration = configuration
        self.primary = Mongo(configuration, is_primary=True)
        self.secondary = Mongo(configuration, is_primary=False)

    """
        In charge of launching the entire synchronisation of every database. Simple version without any multi-threading.
    """
    def start(self):
        print('Prepare sync of the following databases: '+str(', '.join(self.primary.list_databases())))

        # Check all CollectionParts we need to create
        oplog_input = None
        other_inputs = []
        for db in self.primary.list_databases():
            for coll in self.primary.list_collections(db):
                collection = Collection(configuration=self.configuration, db=db, coll=coll)
                collection_part_inputs = collection.prepare_sync()

                for inputs in collection_part_inputs:
                    # We need to reserve a long-running thread for the oplog. So, we want to put as the first element of the Queue
                    data = {'collection_part': inputs}
                    if db == "local" and coll == "oplog.rs":
                        oplog_input = data
                    else:
                        other_inputs.append(data)

        if oplog_input is None:
            raise ValueError("No oplog found...")

        # Fill queues used for the multi-threading
        qi = mp.Queue()
        qo = mp.Queue()

        qi.put(oplog_input)
        for inputs in other_inputs:
            qi.put(inputs)

        # Starts the Jobs. We need at least 1 thread for the oplog, and another for the other collections
        jobs = []
        jobs_quantity = 1 + int(max(1,self.configuration.internal_threads()))
        common_info = {'configuration_filepath': Configuration.FILEPATH}
        for i in range(int(jobs_quantity)):
            qi.put('DONE')
            job = mp.Process(target=clone_collection_part, args=(qi, qo, i, common_info, ))
            job.start()
            jobs.append(job)

        job_done = 0
        while job_done < (jobs_quantity - 1): # There is one long-running thread which should never finish by itself.
            try:
                res = qo.get(timeout=3600*24)
                if res == 'DONE':
                    job_done += 1
                    print('Remaining jobs: '+str(jobs_quantity - job_done - 1))
            except QueueEmpty: # We cannot put a super-huge time out, so we simply handle the exception
                pass
            except:
                raise  # Raise all other errors

        print('End synchronisation of every database, the oplog synchronisation will continue until you stop this script. Afterwards, just remove the database from the maintenance mode.')

    """
        Create the appropriate CollectionPart instance
    """
    @staticmethod
    def create_collection_part(inputs):
        if inputs['db'] == 'local' and inputs['coll'] == 'oplog.rs':
            return OplogCollectionPart(**inputs)
        else:
            return BasicCollectionPart(**inputs)
