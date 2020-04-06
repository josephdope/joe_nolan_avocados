import os
from datetime import datetime
import pandas as pd
import argparse
import logging
from logging.handlers import TimedRotatingFileHandler
import re
from geopy import geocoders 



logger = logging.getLogger('avocado_pipeline')
logger.setLevel('DEBUG')
log_file_name = "avocado_pipeline"
filename=f'{os.getcwd()}/logs/{log_file_name}_{datetime.now().date()}.log'
fh = TimedRotatingFileHandler(filename=filename, when='midnight')
logFormatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(lineno)04d | %(message)s')
fh.setFormatter(logFormatter)
logger.addHandler(fh)


def split_words(item):
    return re.sub(r"(\w)([A-Z])", r"\1 \2", item)

def make_map(item, geocoder):
    try:
        geocoded = geocoder.geocode(item)
        lat = geocoded.latitude
        longit = geocoded.longitude
        if ((lat < 28.7921169) and (lat > 44.161344)) or ((longit < -102.9114394) and (longit > -83.3079998)):
            error_msg = item + ' - was outside of bounds of continental US.\n'
            logger.error(error_msg)
        return pd.Series([geocoded.latitude, geocoded.longitude])

    except:
        error_msg = item + ' - no match was found for this location'
        logger.error(error_msg)
        return pd.Series([None, None])


class AvocadoPipeline:
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        
    def main(self):
        self._read_file()
        self._create_date_features()
        self._create_location_features()
        self._get_dummies()
        self._write()

    def _read_file(self):
        self.parser.add_argument('-i','--input_file', dest='input_file', action='store',
                    help='location of input file')
        self.parser.add_argument('-o', '--output_loc', dest='output_loc', action='store',
                    help='location to store transformed file')
        args = self.parser.parse_args()
        input_file = args.input_file
        self.df = pd.read_csv(input_file)
        self.output_dir = args.output_loc

    def _create_date_features(self):
        self.df = pd.read_csv('../data/avocado.csv')
        self.df = self.df[[col for col in self.df.columns if col.startswith('Unnamed') ==  False]]
        self.df['Date'] = pd.to_datetime(self.df['Date'], format='%Y/%m/%d')
        self.df['Month'] = self.df['Date'].apply(lambda x: str(x.month))
        self.df['Week'] = self.df['Date'].apply(lambda x: str(x.week))

    def _create_location_features(self):
        nom = geocoders.Nominatim(user_agent='avocados')
        unique_regions = pd.DataFrame(self.df['region'].unique(), columns=['region'])
        unique_regions['region_tmp'] = unique_regions['region'].apply(lambda x: split_words(x))
        unique_regions[['lat', 'long']] = unique_regions['region_tmp'].apply(lambda x: make_map(x, nom))
        self.df = self.df.merge(unique_regions, on='region', how = 'left')


    def _get_dummies(self):
        obj_types = self.df.select_dtypes(include=['object'])
        dummied = pd.get_dummies(obj_types,prefix=obj_types.columns, drop_first=True)
        self.df = self.df.merge(dummied, left_index=True, right_index=True)

    def _write(self):    
        self.df.to_csv(self.output_dir, index = False)

if __name__ == '__main__':
    avocado_pipeline = AvocadoPipeline()
    avocado_pipeline.main()

    