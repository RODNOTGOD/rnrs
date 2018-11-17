#!/usr/bin/env python3

from openpyxl import load_workbook
import glob
import json
import pandas as pd
import numpy as np
import os
import urllib

wb = load_workbook(filename='Downloads/1_year_Mini_Geo.xlsx')

new_df = pd.DataFrame(columns=['state', 'area', 'income', 'geocode', 'url', 'error'])

state_abbv = {'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA',
              'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA',
              'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
              'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA',
              'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT',
              'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM',
              'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
              'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD',
              'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA',
              'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY', 'American Samoa': 'AS', 'District of Columbia': 'DC',
              'Federated States of Micronesia': 'FM', 'Guam': 'GU', 'Marshall Islands': 'MH', 'Northern Mariana Islands': 'MP',
              'Palau': 'PW', 'Puerto Rico': 'PR', 'Virgin Islands': 'V'}

os.chdir('git/gun-violence-data/intermediate')


def get_city_metadata(city_geocode):
    data = None
    return data


files = glob.glob('stage2*.csv')
for file in files:
    df = pd.read_csv(file)
    print(f'reading: {file}')
    for index, row in df[['state', 'city_or_county']].iterrows():
        st_abbv = state_abbv[row['state']].lower()
        ws = wb[st_abbv]
        state_geocodes = pd.DataFrame(list(ws.values)[1:], columns=['State', 'Logical Record Number', 'Geography ID', 'GeographyName'])
        code = state_geocodes[state_geocodes.GeographyName.str.contains(row['city_or_county'])]['Geography ID']
        if not code.empty:
            metadata = get_city_metadata(list(code)[0])
            if type(metadata) is dict:
                print('ok')
                new_df = new_df.append({'state': row['state'], 'area': row['city_or_county'], 'income': metadata['value'], 'url': list(code)[0], 'geocode': metadata['url'], 'error': np.NAN}, ignore_index=True)
            else:
                new_df = new_df.append({'state': row['state'], 'area': row['city_or_county'], 'income': np.NAN, 'url': metadata, 'geocode': list(code)[0], 'error': 'http 500 error' }, ignore_index=True)
        else:
            new_df = new_df.append({'state': row['state'], 'area': row['city_or_county'], 'income': np.NAN, 'url': np.NAN, 'geocode':np.NAN, 'error':'No geoid found'}, ignore_index=True)
new_df.to_csv('/home/kragendora/city_income_medians.csv')
