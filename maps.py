import os
from functools import cached_property
from dataclasses import dataclass
from dotenv import load_dotenv
import requests
import googlemaps
from typing import Tuple, Set
import datetime as dt
from pathlib import Path
import pandas as pd
import re
from fuzzywuzzy import fuzz

load_dotenv()


@dataclass
class CoordinateBox:
    # Latitude, Longitude order
    south_east: Tuple[float, float]
    south_west: Tuple[float, float]
    north_west: Tuple[float, float]
    north_east: Tuple[float, float]


class Borough:

    def __init__(self, name: str):
        self.name = name
        self.longitude_latitude_box = None
        self.zip_codes = None

    def __repr__(self):
        return f"Borough(name={self.name})"

    @property
    def longitude_latitude_box(self) -> CoordinateBox:
        return self._longitude_latitude_box

    @longitude_latitude_box.setter
    def longitude_latitude_box(self, coordinate_box: CoordinateBox):

        self._longitude_latitude_box = coordinate_box

    @property
    def zip_codes(self) -> Set[int]:
        return self._zip_codes

    @zip_codes.setter
    def zip_codes(self, zip_codes: Set[int]):
        self._zip_codes = zip_codes


class AllBoroughs:
    zip_code_file_path = Path(
        r"C:\Users\austi\Documents\GitHub_Repos\Public_Projects\Case Studies\P72\data\borough_zip_codes.csv")
    borough_names = ['Manhattan', 'Queens', 'Brooklyn', 'Bronx', 'Staten Island']
    coordinate_boxes = {
        'Staten Island': CoordinateBox(
            (40.4660573119, -74.1834290139), (40.5204589925, -74.2844206765), (40.6772933017, -74.1381104104),
            (40.6230190293, -74.0371187479)
        ),
        'Manhattan': CoordinateBox(
            (40.6927339606, -73.9888462559), (40.7079726717, -74.0336026352), (40.8503448578, -73.9491631632),
            (40.8351387679, -73.9044067839)
        ),
        'Bronx': CoordinateBox(
            (40.7707306943, -73.8351585942), (40.8083064286, -73.9456672913), (40.9074320111, -73.886807152),
            (40.869912442, -73.7762984549)
        ),
        'Brooklyn': CoordinateBox(
            (40.5418655429, -73.9194536632), (40.5917712318, -74.065734766), (40.7420403413, -73.9767618867),
            (40.6922469184, -73.8304807839)
        ),
        'Queens': CoordinateBox(
            (40.6294568904, -73.8362079488), (40.7508541298, -73.9640613265), (40.8340243272, -73.8264951108),
            (40.7127788742, -73.6986417331)
        )
    }

    @cached_property
    def zip_code_data(cls):
        data = pd.read_csv(cls.zip_code_file_path, dtype={'zip_code': int, 'borough': str})
        return data

    def set_boroughs_as_attrs(self):
        for borough_name in self.borough_names:
            borough = Borough(name=borough_name)
            borough.longitude_latitude_box = self.coordinate_boxes.get(borough_name)
            borough.zip_codes = set(self.zip_code_data.loc[lambda x: x.borough == borough_name].zip_code.unique())
            setattr(self, borough_name, borough)
        return self


class GroceryStores:

    comparable_store_names = [
        "Trader Joe's",
        "Whole Foods Market",
        "Wegmans",
        "Morton Williams",
        "Fairway Market",
        "NatureBox",
        "Meijer"
    ]

    def __init__(self):
        self.gmaps: googlemaps.client = googlemaps.Client(key=os.getenv("GOOGLE_MAPS_KEY"))
        # self.all_boroughs = AllBoroughs().set_boroughs_as_attrs()

    def get_store_locations_in_boroughs_from_gmaps(self, store_name: str, borough_name: str) -> pd.DataFrame:
        base_url = "https://maps.googleapis.com/maps/api/place/textsearch/json?"
        query = f"{store_name} in {borough_name}, NY"
        params = dict(
            query=query,
            type='Grocery Store',
            key=os.getenv("GOOGLE_MAPS_KEY")
        )
        req = requests.models.PreparedRequest()
        req.prepare_url(url=base_url, params=params)
        response = requests.request('GET', req.url)

        data = pd.json_normalize(response.json().get('results'))

        return data

    # TODO: The Rectangular Search Functionality with the Find Places API Endpoint was not working - noted bug/lack of
    #  feature as of 2020 based on Github tickets
    # def find_stores_in_borough(self, store_name: str, borough: Borough):
    #     sw, ne = borough.longitude_latitude_box.south_west, borough.longitude_latitude_box.north_east
    #     # Required format of rectangle:south,west|north,east
    #     borough_location_bias = f'rectangle:{sw[0]},{sw[1]}|{ne[0]},{ne[1]}'
    #     fields = [
    #         'formatted_address', 'place_id', 'price_level', 'rating', 'user_ratings_total'
    #     ]
    #     places = self.gmaps.find_place(
    #         input=store_name, input_type='textquery',
    #         fields=fields,
    #         location_bias=borough_location_bias
    #     )

    def aggregate_comparable_store_data(self, save_data: bool = False) -> pd.DataFrame:

        all_data = []
        file_name = Path(f"nyc_store_data_{dt.datetime.now().__format__('%Y%m%d-%H_%M_%S')}.csv")
        dir_path = Path(r"C:\Users\austi\Documents\GitHub_Repos\Public_Projects\Case Studies\P72\data")

        for borough_name in AllBoroughs.borough_names:

            for store in self.comparable_store_names:

                try:
                    data = self.get_store_locations_in_boroughs_from_gmaps(store, borough_name)
                    all_data.append(data)
                except:
                    logger.error(f"Error Occurred when querying store: {store}")
                    logger.info(f"Saving current data down...")
                    aggregated_data = pd.concat(all_data, axis=0)
                    if save_data:
                        aggregated_data.to_csv(dir_path.joinpath(file_name), index=False)
                    return aggregated_data
            try:
                aggregated_data = pd.concat(all_data, axis=0)
                if save_data:
                    aggregated_data.to_csv(dir_path.joinpath(file_name), index=False)
                return aggregated_data

            except:
                logger.error(f"Error when saving the final Agg'd DF")

    def get_latest_saved_comparable_store_data(self):
        dir_path = Path(r"C:\Users\austi\Documents\GitHub_Repos\Public_Projects\Case Studies\P72\data")
        all_files = sorted(list(dir_path.glob(r"nyc_store_data_*.csv")))
        data = pd.read_csv(all_files[-1])
        return data

    def clean_up_comparable_store_data(self, data: pd.DataFrame) -> pd.DataFrame:
        selected_fields = [
            'business_status',
            'formatted_address',
            'zip_code',
            'borough',
            'standardized_name',
            'user_ratings_total',
            'place_id',
            'price_level',
            'rating',
            'lat',
            'lon',
        ]

        column_rename = {
            'geometry.location.lat': 'lat',
            'geometry.location.lng': 'lon'
        }

        def standardize_store_name(observed_name: str):
            ratios = [fuzz.partial_ratio(observed_name, c) for c in self.comparable_store_names]
            return self.comparable_store_names[ratios.index(max(ratios))]

        zip_code_pattern = re.compile("\d{5}")
        cleaned_data = (
            data
            .rename(columns=column_rename)
            .assign(
                zip_code=lambda x: x.formatted_address.apply(lambda y: zip_code_pattern.findall(y).pop()).astype(int),
                standardized_name=lambda x: x.name.apply(standardize_store_name)
            )
            .merge(AllBoroughs().zip_code_data, how='left', on='zip_code')
            .loc[:, selected_fields]
            .drop_duplicates(subset=['formatted_address'])
            .loc[lambda x: x.borough.notna()]
        )
        return cleaned_data

    def plot_stores_to_map(self):
        pass





if __name__ == '__main__':
    from P72.utils import logger

    all_boroughs = AllBoroughs().set_boroughs_as_attrs()

    stores = GroceryStores()
    # agg_store_data = stores.aggregate_comparable_store_data(save_data=True)
    latest_store_data = stores.get_latest_saved_comparable_store_data()
    clean_store_data = stores.clean_up_comparable_store_data(latest_store_data)
