from src.core.service.Mongo import Mongo
from src.core.clone.Collection import Collection
from src.core.clone.BasicCollectionPart import BasicCollectionPart
from src.core.clone.OplogCollectionPart import OplogCollectionPart

class Core:

    def __init__(self, configuration):
        self.configuration = configuration
        self.primary = Mongo(configuration, is_primary=True)
        self.secondary = Mongo(configuration, is_primary=False)

    """
        In charge of launching the entire synchronisation of every database. Simple version without any multi-threading.
    """
    def start(self):
        print('Start sync of the following databases: '+str(', '.join(self.primary.list_databases())))
        for db in self.primary.list_databases():
            print('Start sync of all collections from the database: '+db)
            for coll in self.primary.list_collections(db):
                collection = Collection(configuration=self.configuration, db=db, coll=coll)
                collection_part_inputs = collection.prepare_sync()

                for inputs in collection_part_inputs:
                    inputs['configuration'] = self.configuration
                    collection_part = self.create_collection_part(inputs)
                    collection_part.sync()

        print('End synchronisation of every database.')

    """
        Create the appropriate CollectionPart instance
    """
    def create_collection_part(self, inputs):
        if inputs['db'] == 'local' and inputs['coll'] == 'oplog.rs':
            return OplogCollectionPart(**inputs)
        else:
            return BasicCollectionPart(**inputs)
