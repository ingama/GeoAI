import LOAD_GHCN_DATA as GHCN
import os
import pathlib
import pandas as pd

#creating a path for a Windows or UNIX environment - replace with your own path
if os.name == 'nt':
    home_directory = "D:/" +  "/ClimateData/Data/Monthly/Temperature/Unadjusted/" 
else: 
    home_directory = os.path.expanduser('~') +  "/ClimateData/Data/Monthly/Temperature/Unadjusted/" 

location = pathlib.Path(home_directory)

file_name1 = "temperature.txt"
file_name2 = "stations.txt"

mdf, msf =  GHCN.monthly_temperature_data_stations_metadata(location,file_name1,file_name2)

#converting data attributes to indexes
mdf.set_index(['COUNTRY','ID', 'YEAR'], inplace = True)
msf.set_index('ID', inplace = True)

pd.set_option("display.precision", 2)
display(mdf, msf)