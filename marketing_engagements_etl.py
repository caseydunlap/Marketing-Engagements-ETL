import pandas as pd
import pytz
from datetime import datetime,timedelta,timezone,time,date
from dateutil.relativedelta import relativedelta
import requests
import snowflake.connector
from sqlalchemy import create_engine
import json
import os
import logging

#Config logging
script_dir = os.path.dirname(os.path.realpath(__file__))
logging.basicConfig(
    filename=os.path.join(script_dir,'logs.log'),
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

try:
    #Fetch secrets
    object_type_id = os.environ.get('hubspot_marketing_enagements_object_id')
    access_token = os.environ.get('hubspot_etl_access_token')
    snowflake_user = os.environ.get('snowflake_user')
    snowflake_pass = os.environ.get('snowflake_password')
    snowflake_role = os.environ.get('snowflake_role')
    snowflake_fivetran_wh = os.environ.get('snowflake_fivetran_wh')
    snowflake_fivetran_db = os.environ.get('snowflake_fivetran_db')
    snowflake_account = os.environ.get('snowflake_account')

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    #Store results from lists endpoint request
    all_results = []
    initial_url = 'https://api.hubapi.com/crm/v3/lists/9705/memberships'

    next_url = initial_url

    #Paginate through until payload is complete
    while next_url:
        response = requests.get(next_url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            #Append current batch of results
            all_results.extend(data.get('results', []))  

            #Check if there's a next page and update the next_url for the next request
            paging_info = data.get('paging', {}).get('next')
            if paging_info:
                #Use this link for the next request
                next_url = paging_info['link']  
            else:
                next_url = None
        else:
            #Error handling
            next_url = None

    #Init snowflake session
    ctx = snowflake.connector.connect(
        user = snowflake_user,
        role = snowflake_role,
        warehouse = snowflake_fivetran_wh,
        password = snowflake_pass,
        schema = 'HUBSPOT',
        account= snowflake_account)

    #Fetch already existing marketing engagement object ids from data table
    cs = ctx.cursor()
    script = f"""
    select * from "{snowflake_fivetran_db}"."HUBSPOT"."MARKETING_ENGAGEMENTS"
    """
    payload = cs.execute(script)
    current_df = pd.DataFrame.from_records(iter(payload), columns=[x[0] for x in payload.description])

    #Store ids from data table in list
    df_id_list = current_df['ID'].tolist()
    #Compare ids from List request with IDs in table
    df_list_integers = [int(x) for x in df_id_list]

    #Store non-matches in list
    non_matches = [x for x in all_results if x not in df_list_integers]

    #Batch request applicable for only net new engagement ids
    batch_read_url = f'https://api.hubapi.com/crm/v3/objects/{object_type_id}/batch/read'

    properties = ["hs_createdate", "company_name", "email_address", "email_name", "engagement_date", "first_name", "last_name", "form_name",
                "hubspot_contact_record_id", "icapture_lead_rating", "lead_source___most_recent", "marketing_engagement_type",
                "partner_of_interest", "salesforce_account_id", "salesforce_campaign_id", "salesforce_campaign_name",
                "salesforce_contact_id", "salesforce_lead_id", "url"]

    #Max batch size
    batch_size = 100

    detailed_data_list = []

    #Chunk the non-matches list into smaller batches and send requests
    for i in range(0, len(non_matches), batch_size):
        batch_ids = non_matches[i:i + batch_size]

        inputs = [{"id": id} for id in batch_ids]

        #JSON body to specify request properties and input parameters
        data = {
            "properties": properties,
            "inputs": inputs
        }

        #Send the request
        response = requests.post(batch_read_url, headers=headers, data=json.dumps(data))

        if response.status_code == 200:
            detailed_data = response.json()
            for item in detailed_data['results']:
                row = {'id': item['id']}
                row.update(item['properties'])
                detailed_data_list.append(row)
        else:
            break
            #Should only execute if error, error will log in logging file
    
    #Store results from batch request in pandas dataframe
    update_df = pd.DataFrame(detailed_data_list)

    #Convert datetime fields to UTC, without manually specifying format
    update_df['hs_createdate'] = pd.to_datetime(update_df['hs_createdate'], errors='coerce', utc=True)
    update_df['hs_lastmodifieddate'] = pd.to_datetime(update_df['hs_lastmodifieddate'], errors='coerce', utc=True)

    #Define EST
    est_timezone = pytz.timezone("US/Eastern")

    #Convert UTC time to EST; datetime objects are already tz-aware due to `utc=True`
    update_df['hs_createdate_est'] = update_df['hs_createdate'].dt.tz_convert(est_timezone)
    update_df['hs_lastmodifieddate_est'] = update_df['hs_lastmodifieddate'].dt.tz_convert(est_timezone)

    #Format the datetime objects as strings in EST, including only the timezone offset
    update_df['hs_createdate_est_str'] = update_df['hs_createdate_est'].dt.strftime("%Y-%m-%d %H:%M:%S %z")
    update_df['hs_lastmodifieddate_est_str'] = update_df['hs_lastmodifieddate_est'].dt.strftime("%Y-%m-%d %H:%M:%S %z")

    #Do some minor data transformation
    update_df.drop(['hs_createdate', 'hs_lastmodifieddate', 'hs_createdate_est_str', 'hs_lastmodifieddate_est_str','hs_object_id'], axis=1, inplace=True)
    update_df.rename(columns={'hs_createdate_est': 'hs_createdate', 'hs_lastmodifieddate_est': 'hs_lastmodifieddate'}, inplace=True)

    update_df.rename(columns={'hubspot_contact_record_id': 'hs_contact_id'}, inplace=True)
    update_df.drop(columns='hs_lastmodifieddate', inplace=True)

    update_df.columns = update_df.columns.str.upper()

    #Instantiate SQLalchemy engine
    engine = create_engine(f'snowflake://{snowflake_user}:{snowflake_pass}@{snowflake_account}/{snowflake_fivetran_db}/HUBSPOT?warehouse={snowflake_fivetran_wh}&role={snowflake_role}')

    chunk_size = 10000
    chunks = [x for x in range(0, len(update_df), chunk_size)] + [len(update_df)]
    table_name = 'marketing_engagements' 

    for i in range(len(chunks) - 1):
        print(f"Inserting rows {chunks[i]} to {chunks[i + 1]}")
        update_df[chunks[i]:chunks[i + 1]].to_sql(table_name, engine, if_exists='append', index=False)
    logging.getLogger().setLevel(logging.INFO)
    logging.info('Success')
except Exception as e:
    logging.exception('Operation failed due to an error')
logging.getLogger().setLevel(logging.ERROR)
