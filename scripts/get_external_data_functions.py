# Importing necessary libraries:

import geopandas
import folium
import io
import os
import requests
import zipfile
import pandas as pd

from pyspark.sql import SparkSession, functions as F
from urllib.request import urlretrieve
from owslib.wfs import WebFeatureService

# Setting data paths:
root_dir = './data/tables/'
external_data_dir = 'external_datasets'
path = root_dir + external_data_dir + '/'


def urlretrieve_data(URL, output_file_name, zip):
    '''
    Function to retrieve data based on arguments listed below:

    1) URL: URL of the site where the data is being retrieved from.
    2) output_file_name: name of the output file that will be 
    saved in the target directory.
    3) zip: a boolean value that can take the value of 0 or 1.
    * 0 --> implies that data being retrieved IS NOT a zip file.
    * 1 --> implies that data being retrieved IS a zip file.

    '''

    # if unzipping is not required
    if (zip == 0):
        r = requests.get(URL)
        target_dir = path + output_file_name

        with open(target_dir, 'wb') as outfile:
            outfile.write(r.content)
            outfile.close()

    # if unzipping is required
    elif (zip == 1):
        target_dir = path + output_file_name
        urlretrieve(URL, target_dir)

        # unzip zip file
        with zipfile.ZipFile(target_dir,"r") as zip_ref:
            zip_ref.extractall(path + output_file_name[:-4])

    return None

def create_external_data_directory():
    '''
    Creates directory to store external data
    '''

    external_data_dir = 'external_datasets'

    if not os.path.exists(root_dir + external_data_dir):
        os.makedirs(root_dir + external_data_dir)
    
    return None

def get_postcode_SA2_data():
    '''
    gets postcode-SA2 correspondence data
    '''

    urlretrieve_data(URL = "https://www.matthewproctor.com/Content/"+\
                    "postcodes/australian_postcodes.csv",
                    output_file_name = 'postcode_SA2_data.csv',
                    zip = 0)

    return None


def get_income_data():
    '''
    gets income dataset (2014-2019)
    '''


    urlretrieve_data(URL = "https://www.abs.gov.au/statistics/"+\
                    "labour/earnings-and-working-conditions/"+\
                    "personal-income-australia/2014-15-2018-19/"+\
                    "6524055002_DO001.xlsx",
                    output_file_name = 'income_data.xlsx',
                    zip = 0)

    return None

def get_state_shapefiles():
    '''
    gets shapefiles of australian states
    '''

    urlretrieve_data(URL = "https://www.abs.gov.au/statistics/standards/"+\
                    "australian-statistical-geography-standard-asgs-"+\
                    "edition-3/jul2021-jun2026/access-and-downloads/"+\
                    "digital-boundary-files/STE_2021_AUST_SHP_GDA2020.zip",
                    output_file_name = 'state_data.zip',
                    zip = 1)

    return None

def get_postcode_shapefiles():
    '''
    gets shapefile of Australian post codes
    '''

    urlretrieve_data(URL = "https://www.abs.gov.au/statistics/"+\
                    "standards/australian-statistical-geography-"+\
                    "standard-asgs-edition-3/jul2021-jun2026/"+\
                    "access-and-downloads/digital-boundary-files/"+\
                    "POA_2021_AUST_GDA94_SHP.zip",
                    output_file_name = 'postcode_data.zip',
                    zip = 1)
    
    return None


def get_population_data():
    '''
    gets population data (2001-2021) from API
    '''

    # Set up API connection.
    WFS_USERNAME = 'xrjps'
    WFS_PASSWORD= 'Jmf16l4TcswU3Or7'
    WFS_URL='https://adp.aurin.org.au/geoserver/wfs'

    adp_client = WebFeatureService(url=WFS_URL,username=WFS_USERNAME,
        password=WFS_PASSWORD, version='2.0.0')
    
    # Extract files and store into external dataset folder directory.
    response = adp_client.getfeature(
        typename='datasource-AU_Govt_ABS-UoM_AURIN_DB_3:"+\
            "abs_regional_population_sa2_2001_2021', 
        outputFormat='csv')
    target_dir = path + 'population_data.csv'

    # save data
    out = open(target_dir, 'wb')
    out.write(response.read())
    out.close

    return None


