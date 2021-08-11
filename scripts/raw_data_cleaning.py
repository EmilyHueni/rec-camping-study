import boto3
import os
import pandas as pd
import dask.dataframe as dd



s3 = boto3.client("s3")
bucket = 'rec-gov-study'


#define s3 location keys
Campsites_API_file = 'data/Campsites_API_v1.csv'
fy20_historical_reservations_file = 'data/fy20_historical_reservations_full.csv'


#create df from reservation data on s3 bucket. 
#this is a large file and may take a few minutes
boto_object = s3.get_object(Bucket=bucket, Key=fy20_historical_reservations_file)
date_cols = ['startdate', 'enddate', 'orderdate']
df_all_res = pd.read_csv(boto_object['Body'], parse_dates=date_cols, nrows=100000)


print('dowloaded all reservation records from s3')

#create df from campsite api data on s3 bucket.
boto_object = s3.get_object(Bucket=bucket, Key=Campsites_API_file)
df_campsite_api = pd.read_csv(boto_object['Body'])

print('dowloaded campsite api from s3')

#coerce dates for endate
df_all_res['enddate'] = pd.to_datetime(df_all_res['enddate'], errors='coerce')


#looking at the campsite api, how many campsites are at each facility
df_facility_max = df_campsite_api.groupby('FacilityID')['CampsiteID'].count().reset_index()
df_facility_max.columns = ['facilityid', 'total_num_campsites']

print('created new facility max dataframe')

#Identify what reservations are campsite related based on facility ID
df_merge_all_data = pd.merge(df_all_res, df_facility_max, on='facilityid', how='left')
df_merge_all_data['campsite'] = ~df_merge_all_data['total_num_campsites'].isnull()

print('merged dataframes')

ddf = dd.from_pandas(df_merge_all_data, npartitions=4)
print('transformed into dask dataframe')

def reservations_likely_canceled(row):
    '''if there is another reservation made at a later date that has the same facility and 
    productuctid and is within the same reservation date block (between start and end) then 
    consider this one to be a likely canceled one.'''
    #print(row['index'])
    print(row['facilityid'])
    
    if row['campsite'].item() == False:
        return False
    else:
        facil = row['facilityid']
        campsite = row['productid']
        sdate = row['startdate']
        edate = row['enddate']
        odate = row['orderdate']
        
        
        #select all rows where the start date is after and facil and campsite number are the same
        #(StartA <= EndB) and (EndA >= StartB)
        df_other_res = ddf[(ddf['facilityid']==facil) & 
                      (ddf['productid']==campsite) & 
                      (ddf['startdate']<=edate) &
                      (ddf['enddate']>=sdate) &
                      (ddf['orderdate']>odate)]
        
        if len(df_other_res.index) == 0:
            return False
        else:
            return True
    

#df_merge_all_data.reset_index(inplace=True)


#df_merge_all_data['cancelation_likely'] = df_merge_all_data.apply(reservations_likely_canceled, axis=1)
ddf['cancelation_likely'] = ddf.map_partitions(reservations_likely_canceled, meta=(None, '?')).compute()

output_file = 'fy20_historical_reservations_full_test_cancel.csv'
ddf.to_csv(output_file)
s3.upload_file(output_file, bucket, output_file)
