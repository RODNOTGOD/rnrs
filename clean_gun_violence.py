#!/usr/bin/env python3

import threading
import math
import glob
import pandas as pd

from scipy.spatial.distance import squareform, pdist


def chunks(lis, chunk_size):
    """Yield successive n-sized chunks from l."""
    for pos in range(0, len(lis), chunk_size):
        yield lis[pos:pos+chunk_size]

def get_city_data(dataset):
    for index in dataset:
        index_mask = (full_merged_data.city_or_county == index) & (full_merged_data.state == state)
        print(state, index)
        row = state_distances[index]
        if isinstance(row, pd.DataFrame):
            row = row.iloc[:, 0]
        grouped_row = row[row < 1]
        adjacent_area = state_data[state_data.city_or_county.isin(grouped_row.groupby(grouped_row.index).first().index)]
        data = {}
        for replacement in replace_rows:
            if adjacent_area[replacement].dtype == object:
                non_null_area = adjacent_area[replacement][adjacent_area[replacement].notna()]
                avg_item = '||'.join(non_null_area.mode())
            else:
                avg_item = adjacent_area[replacement].median()
            data[replacement] = avg_item
        index_row = full_merged_data.loc[index_mask, data.keys()]
        with DATA_LOCK:
            full_merged_data.loc[index_mask, data.keys()] = index_row.fillna(data)

THREAD_COUNT = 4
DATA_LOCK = threading.Lock()

gun_violence_files = sorted(glob.glob('./gun-violence-data/intermediate/stage2*.csv'))

separate_frames = []
for csv_file in gun_violence_files:
    df = pd.read_csv(csv_file)
    separate_frames.append(df)
aggregated_dataframes = pd.concat(separate_frames)
city_data = pd.read_csv('./city-data/final_city_data.csv')

# the dataset to clean
full_merged_data = aggregated_dataframes.merge(city_data, on=['state', 'city_or_county'], how='left', copy=False)

# rows to replace and clean
replace_rows = ['participant_age', 'n_killed', 'n_injured', 'population', 'poverty rate',
                'income', 'age', 'property value', 'employees', 'wage']

# Ignore all areas with lat,long. we can't do distance on areas without
df_with_lat_long = full_merged_data[~full_merged_data[['latitude', 'longitude']].isna().any(axis=1)]
states = df_with_lat_long.state.unique()
for state in states:
    state_data = df_with_lat_long[df_with_lat_long.state == state]
    # Get the ecludian distance from the objects
    state_distances = pd.DataFrame(squareform(pdist(state_data[['latitude', 'longitude']])),
                                   columns=state_data.city_or_county,
                                   index=state_data.city_or_county)

    city_chunk = chunks(state_distances.columns.unique(), math.ceil(len(state_distances.columns.unique()) / THREAD_COUNT))

    thread_pool = []
    for chunk in city_chunk:
        thread_proc = threading.Thread(target=get_city_data, args=(chunk,))
        thread_pool.append(thread_proc)
        thread_proc.start()

    for thread in thread_pool:
        thread.join()
