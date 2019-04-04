import pandas as pd
from dfply import *

df = pd.read_csv("./data/parcel_common_columns_2004_2014.csv")
common_columns = df["0"].to_list()

lakes = pd.read_csv("../MinneMUDAC_raw_files/mces_lakes_1999_2014.csv", 
                      header=0,
                      dtype = {'longitude': str,'latitude': str},
                      sep=',',
                      engine='python')

ref = pd.read_csv("../MinneMUDAC_raw_files/Parcel_Lake_Monitoring_Site_Xref.txt", 
                      header=0,
                      dtype = {'centroid_lat': str,'centroid_long': str},
                      sep='\t',
                      engine='python')

dnr_codes = set(lakes.DNR_ID_Site_Number)
monit_codes = set(ref.Monit_MAP_CODE1)
intersect_codes = dnr_codes.intersection(monit_codes)

lake_table = (lakes >>
    select(X.DNR_ID_Site_Number, X.LAKE_NAME) >>
    group_by(X.DNR_ID_Site_Number,X.LAKE_NAME) >>
    summarize(count = n(X.DNR_ID_Site_Number)))

ll_df = pd.read_csv("./common_latlongs.csv",names=["Lat","Long"])
intersect_lat_long = set(zip(ll_df.Lat,ll_df.Long))

ll_code_dict = {key:val for key,val in zip(intersect_lat_long,ref.Monit_MAP_CODE1)}
ll_dist_dict = {key:val for key,val in zip(intersect_lat_long,ref.Distance_Parcel_Lake_meters)}
code_name_dict = {key:val for key,val in zip(lake_table.DNR_ID_Site_Number,lake_table.LAKE_NAME)}


ll_idnamedist_dict = {ll:(code_name_dict[code],ll_code_dict[ll],ll_dist_dict[ll]) 
                      for ll,code in ll_code_dict.items() 
                      if ll in intersect_lat_long and code in intersect_codes}