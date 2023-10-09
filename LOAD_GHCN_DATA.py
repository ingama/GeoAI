"""
File: LOAD_GHCN_DATA.py
Web: https://gitlab.com/-/snippets/2590561
Author: inga mazaeva
Description: 
Learn how to import the csv, txt, dly Monthly & Daily data of Global Historical Climatology Network into a Pandas DataFrame.
FOR EDUCATIONAL PURPOSES ONLY
"""

import pandas as pd
import os
import numpy as np

def monthly_temperature_data_stations_metadata(location, file_name1, file_name2):
    
    """
    GLOBAL HISTORICAL CLIMATOLOGY NETWORK - MONTHLY (GHCN-M) 
    Version 4
    Code for reading GHCN .txt file data type of ALL STATIONS
    DESCRIPTION OF DATA
    ID  = Station identification code
    YEAR = 4 digit year of the station record
    ELEMENT = element type, monthly mean temperature="TAVG"
    VALUE: monthly average temperature (hundredths of a degree Celsius)
    MISSING VALUES =-9999
    """
    
    path = location / file_name1
    
    data_header_specs  = [
        (0,2),
        (0,11),
        (11,15),
        (19,24),
        (27,32),
        (35,40),
        (43,48),
        (51,56),
        (59,64),
        (67,72),
        (75,80),
        (83,88),
        (91,96),
        (99,104),
        (107,112)]

    data_header_names = [
        "COUNTRY",
        "ID", 
        "YEAR"
        ] + [
        "M" + str(i + 1) for i in range(12)]
    
    data_header_dtypes = {
        "COUNTRY": str,
        "ID": str, 
        "YEAR": 'int32'}
   
    # In many data science applications, the precision offered by Float64 is not necessary,
    # and using Float32 can save significant memory and speed up computations.

    data_col_dtypes = [{
        "M" + str(i + 1): 'float32'}     
        for i in range(12)]
    data_header_dtypes.update({i: j for m in data_col_dtypes for i, j in m.items()})
    
    mdf = pd.read_fwf(path, colspecs = data_header_specs, names = data_header_names, dtype = data_header_dtypes)
 
    mdf.iloc[:, 3 : 15] *=np.float32(0.01)
 
    #print(f"{np.float32(0.01):.64f}")   
    #print(f"{np.float64(0.01):.64f}")
    #Select columns with 'float64' dtype  
    #float64_cols = list(mdf.select_dtypes(include='float64'))
  
    mdf = mdf.round(2)
    mdf.replace(np.float32(-99.99), np.nan, inplace=True)
    
    path = os.path.join(location,file_name2)
    
    metadata_names = [
        "ID", 
        "LATITUDE", 
        "LONGITUDE", 
        "ELEVATION", 
        "NAME", 
        "TMP"]
    
    metadata_dtype = {
        "ID": str, 
        "LATITUDE": 'float32', 
        "LONGITUDE": 'float32', 
        "ELEVATION": 'float32', 
        "NAME": str, 
        "TMP": str}

    msf = pd.read_csv(path, header = None,  names = metadata_names, dtype = metadata_dtype, delim_whitespace = True, decimal = ".", 
                     usecols = [i for i in range(5)])
    
    #assign country labels
    msf = msf.assign(COUNTRY = msf.ID.str[0:2]) 
    
    return mdf, msf


def daily_temperature_precipitation_data(location, file_name):
    
    """
    DAILY GLOBAL HISTORICAL CLIMATOLOGY NETWORK (GHCN-DAILY)
    Version 3.30 
    Code for reading GHCN .dly file data type of ONE SELECTED STATION 
    
    DESCRIPTION OF DATA
    PRCP = Precipitation (tenths of mm)
    TMAX = Maximum temperature (tenths of a degree Celsius)
    TMIN = Minimum temperature (tenths of a degree Celsius)
    TAVG = Average temperature (tenths of a degree Celsius)
    MISSING VALUES =-9999
    """
    
    path = os.path.join(location, file_name)

    data_header_col_specs = [
        (0,  11),
        (11, 15),
        (15, 17),
        (17, 21)]
    
    data_header_names = [
        "ID", 
        "YEAR",
        "MONTH",
        "ELEMENT"]
    
    data_header_dtypes = {
        "ID": str,
        "YEAR": 'int32',
        "MONTH": 'int32',
        "ELEMENT": str}
    
    data_col_names = [[str(i + 1)] for i in range(31)]
    
    data_col_specs = [[ (21 + i * 8, 26 + i * 8)] for i in range(31)]
    
    data_col_specs = sum(data_col_specs, [])

    data_col_names = sum(data_col_names, [])

    data_col_dtypes = [{str(i + 1): 'float32'}  for i in range(31)]
    
    data_header_dtypes.update({k: v for d in data_col_dtypes for k, v in d.items()})

    ddf = pd.read_fwf(path,
          colspecs=data_header_col_specs + data_col_specs,
          names=data_header_names + data_col_names,
          index_col=data_header_names,
          dtype=data_header_dtypes)

    data_replacement_col_names = [[("VALUE", i + 1)] for i in range(31)]
    data_replacement_col_names = sum(data_replacement_col_names, [])
    data_replacement_col_names = pd.MultiIndex.from_tuples(data_replacement_col_names, names=['TMP', 'DAY'])

    ddf.columns = data_replacement_col_names

    ddf = ddf.loc[:, ('VALUE', slice(None))]
    ddf.columns = ddf.columns.droplevel('TMP')

    ddf = ddf.stack(level='DAY').unstack(level='ELEMENT')

    ddf.replace(-9999.0, np.nan, inplace=True)
    ddf.dropna(how='all', inplace=True)

    ddf.index = pd.to_datetime(
                ddf.index.get_level_values('YEAR') * 10000 +
                ddf.index.get_level_values('MONTH') * 100 +
                ddf.index.get_level_values('DAY'),
                format='%Y%m%d')
    
    ddf *= np.float32(0.1)

    ddf=ddf.round(2)
    return ddf


def daily_stations_metadata(location, file_name):
    
    path = os.path.join(location, file_name)
    
    metadata_col_specs = [
        (0,  12), 
        (12, 21), 
        (21, 31), 
        (31, 38), 
        (38, 41), 
        (41, 72)]
    
    metadata_names = [
        "ID",
        "LATITUDE",
        "LONGITUDE",
        "ELEVATION",
        "STATE",
        "NAME"]
    
    metadata_dtype = {
        "ID": str, 
        "LATITUDE": 'float32',
        "LONGITUDE": 'float32',
        "ELEVATION": 'float32',
        "STATE": str, 
        "NAME": str}
    
    #Doesn't work correctly, format error - for example station ID: US1Madi5459
    dsf = pd.read_fwf(path, colspecs = metadata_col_specs, names = metadata_names, dtype = metadata_dtype)

    return dsf
