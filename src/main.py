from src.core.service.Configuration import Configuration
from src.core.Core import Core
from src.core.service.TestWrite import TestWrite
from src.core.service.TestRead import TestRead
from sys import argv

# python3.6 -m src.main test-write conf/mongosync.json
if __name__ == '__main__':
    if len(argv) <= 1 or argv[1] not in ['start','test-write','test-read']:
        print("Usage: <operation> where operation belongs to 'start', 'test-write', 'test-read'")
        exit(1)
    operation = argv[1]

    if len(argv) == 3:
        configuration_filepath = argv[2]
        Configuration.FILEPATH = configuration_filepath

    configuration = Configuration()

    if operation == 'start':
        core = Core(configuration=configuration)
        core.start()
    elif operation == 'test-write':
        test_write = TestWrite(configuration=configuration)
        test_write.start()
    elif operation == 'test-read':
        test_read = TestRead(configuration=configuration)
        test_read.start()
    else:
        print('Unsupported operation.')
