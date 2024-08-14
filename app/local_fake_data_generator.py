"""
Taken from: https://github.com/eder1985/pismo_recruiting_technical_case/tree/main

Modifications:
    * Added total registries to create
    * Added duplication rate
    * Added execution batches
    * Saving on HDFS
    * Unique filenames

"""
from faker import Faker
from faker.providers import BaseProvider
from datetime import datetime
from json import dumps
import random
import collections
import glob
import os
from hdfs import InsecureClient

BATCHES = 5
TOTAL = 10_000
DUPLICATION_RATE = 0.1
DESTINATION_PATH = '/pismo-data/source/'

class EventTypeProvider(BaseProvider):
    def event_type(self):
        list_event_types = ['account-status-change','transaction-new-value']
        return random.choice(list_event_types)

class StatusTypeProvider(BaseProvider):
    def status_type(self):
        list_status_types = ['ACTIVE','INACTIVE','SUSPENDED','BLOCKED', 'DELETED']
        return random.choice(list_status_types)

class CustomUUIDProvider(BaseProvider):
    def custom_uuid(self):
        list_uuids = [
            '1a1a1a1a-1a1a-1a1a-1a1a-1a1a1a1a1a1a',
            '2b2b2b2b-2b2b-2b2b-2b2b-2b2b2b2b2b2b',
            '3c3c3c3c-3c3c-3c3c-3c3c-3c3c3c3c3c3c'
            ]
        return random.choice(list_uuids)

def custom_data(fake):
    dict_data = {
        "account-status-change": collections.OrderedDict([
            ('id', fake.random_number(digits=6)),
            ('old_status', fake.status_type()),
            ('new_status', fake.status_type()),
            ('reason', fake.sentence(nb_words=5))
        ]),
        "transaction-new-value": collections.OrderedDict([
            ('id', fake.random_number(digits=6)),
            ('account_orig_id', fake.random_number(digits=6)),
            ('account_dest_id', fake.random_number(digits=6)),
            ('amount', fake.pyfloat(positive=True)),
            ('currency', fake.currency_code())
        ])
    }
    return dict_data

def write_fake_data(fake, length, destination_path, unique_uuid = True):
    database = []
    current_time = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = 'fake_events_'+str(fake.uuid4())+'_'+current_time

    for x in range(length):
        uuid = fake.uuid4() if unique_uuid else fake.custom_uuid()
        event_type = fake.event_type()
        project_domain_name = event_type.split('-')[0]

        database.append(collections.OrderedDict([
            ('event_id', uuid),
            ('timestamp', datetime.strftime(fake.date_time_between(start_date='-3y', end_date='now'),"%Y-%m-%dT%H:%M:%S")),
            ('domain', project_domain_name),
            ('event_type', event_type),
            ('data', custom_data(fake).get(event_type))
        ]))

    hdfs_client = InsecureClient(os.getenv('WEBHDFS_NODE'), user=os.getenv('HDFS_USER'))

    with hdfs_client.write('%s%s.json' % (destination_path, filename), encoding='utf-8') as output:
        output.write(dumps(database, indent=4, sort_keys=False, default=str))

    print(f"Local Fake Data Generator: Wrote fake data {length = } {destination_path = } {filename = } {unique_uuid = } ")

def run(length, unique_uuid = True):
    fake = Faker()
    Faker.seed(random.randrange(0, 99999999999999999999, 1))
    fake.add_provider(StatusTypeProvider)
    fake.add_provider(CustomUUIDProvider)
    fake.add_provider(EventTypeProvider)

    write_fake_data(fake, length, DESTINATION_PATH, unique_uuid)

def main(batches = BATCHES, total = TOTAL, duplication_rate = DUPLICATION_RATE):
    for _ in range(batches):
        run(total)
        run(int(total * duplication_rate),unique_uuid = False)

if __name__ == "__main__":
    main()
