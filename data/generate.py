# -*- coding: utf-8 -*-

import json
from faker import Faker

FILES = 10
REGISTRIES = 1000

if __name__ == '__main__':

    f = Faker()

    for i in range(FILES):
        print(i)
        with open(f'./{i:05}.json', 'w') as w:
            w.write(json.dumps([ f.profile() for _ in range(REGISTRIES)], default=str))
    

