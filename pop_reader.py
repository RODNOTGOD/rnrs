#!/usr/bin/env python3
"""
My docstring
"""

import subprocess as sp
import threading
import json
import math

import numpy as np
import pandas as pd

JSON_URL = 'https://api.datausa.io/api/?show=geo&required={field}&geo={geocode}&year=latest'
THREADS = 4

LOCK = threading.Lock()

def chunks(lis, chunk_size):
    """Yield successive n-sized chunks from l."""
    for pos in range(0, len(lis), chunk_size):
        yield lis[pos:pos+chunk_size]


def get_data(codes):
    """Does the thing"""
    global city_df
    fields = {'income': 'income', 'population': 'pop', 'age': 'age', 'employees': 'num_ppl',
              'wage': 'avg_wage', 'poverty rate': 'pop_poverty_status',
              'property value': 'median_property_value'}

    for code in codes:
        data = {'geocode': code}
        print('GEOCODE:', code)
        for field, req_field in fields.items():
            url = JSON_URL.format(geocode=code, field=req_field)
            cmd = sp.Popen(['wget', '-qO-', url], stdout=sp.PIPE, stderr=sp.PIPE)
            cmd.wait()
            try:
                json_file = json.loads(cmd.communicate()[0])
                if 'data' in json_file and json_file['data'] == []:
                    data[field] = json_file['data'][0][2]
                else:
                    data[field] = np.nan
            except IndexError:
                print(f'Failed on {code}: {field}')
            except json.decoder.JSONDecodeError:
                data[field] = np.nan
        with LOCK:
            city_df = city_df.append(pd.Series(data, name='CityValue'))


df = pd.read_csv('city_income_medians.csv')
geocodes = set(df['geocode'].dropna())
cols = ['population', 'poverty rate', 'income', 'age', 'property value', 'employees']
dtypes = ['int', 'float', 'int', 'int', 'int', 'int']

split_data = [i for i in chunks(list(geocodes), math.ceil(len(geocodes) / THREADS))]

print(len(split_data), len(split_data[0]))

threads = []
city_df = pd.DataFrame(columns=['geocode', 'population', 'poverty rate', 'income', 'age', 'property value', 'employees', 'wage'])
for data in split_data:
    tid = threading.Thread(target=get_data, args=(data,))
    threads.append(tid)
    tid.start()

for thread in threads:
    thread.join()
