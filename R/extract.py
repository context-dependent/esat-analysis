import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime
from simple_salesforce import Salesforce
import os
from collections import OrderedDict

load_dotenv()

def cache_path(obj_id):
    obj_root = os.path.join(os.environ.get("Z_HOME"), "Data", "cache", obj_id)
    if not os.path.exists(obj_root):
        os.makedirs(obj_root)
    return obj_root

def latest_cache_file(obj_id): 
    obj_root = cache_path(obj_id)
    files = os.listdir(obj_root).sort()
    if files: 
        return os.path.join(obj_root, files.pop())
    else: 
        return None

def new_cache_file(obj_id, extension = ".csv"): 
    obj_root = cache_path(obj_id)
    return os.path.join(
        obj_root, 
        f"{datetime.now().strftime('%Y%m%d%H%M%S')}{extension}"
    )    

def extract(fetch = False):
    
    if not fetch: 
        cache_file = latest_cache_file("sf")
        if cache_file: 
            print(f"Loading cache file: {cache_file}")
            return pd.read_csv(cache_file)
        else: 
            print("No cache file found. Fetching data from Salesforce.")

    sf_username = os.environ.get('SF_USERNAME')
    sf_password = os.environ.get('SF_PASSWORD')
    sf_securitytoken = os.environ.get('SF_SECURITYTOKEN')

    ### Log into SF
    sf = Salesforce(
        username=sf_username,
        password=sf_password,
        security_token=sf_securitytoken
    )

    ### Declare the SOQL queries. SOQL doesn't support explicit joins, so pull the tables in isolation and 
    ### then join in Python. The fields I've chosen are based on Samridhi's advice about which fields people actually use. 
    full_query = """
    SELECT Id, 
        Program_Survey_ID__c,
        Due_Date__c,
        Survey_Response_ID__c,
        Enrollment__r.Id, 
        Enrollment__r.Date_Of_Enrollment__c,
        Enrollment__r.Gender__c,
        Enrollment__r.Race_Ethnicity__c,
        Enrollment__r.Program_Location__c,
        Enrollment__r.Program_Stream__c,
        Enrollment__r.Cohorts__r.Id,
        Enrollment__r.Cohorts__r.Name, 
        Enrollment__r.Cohorts__r.Start_Date__c,
        Enrollment__r.Cohorts__r.Program__r.Name, 
        Enrollment__r.Participant_Contact__r.FirstName, 
        Enrollment__r.Participant_Contact__r.LastName, 
        Enrollment__r.Participant_Contact__r.Email, 
        Enrollment__r.Participant_Contact__r.Birthdate,
        Enrollment__r.Participant_Contact__r.External_Reference_ID__c
    FROM Participant_SurveyAssessment_Response__c
    WHERE Program_Survey_ID__c IN (
        'SV_3JJ1CYeq4QtkUHI', 
        'SV_4SB1rExRiKUJguW', 
        'SV_5mRwBvh7pBeHGZ0',
        'SV_6KewFmt3GPI7oHA', 
        'SV_7Px0HNcFCeFoUd0'
    )
    """
    
    fq = sf.query_all(query = full_query)
    
    res = sfdc_to_df(fq)
    
    res.columns = [
        'pas_id', 
        'qx_survey_id', 
        'due_date',
        'response_id',
        'enrollment_id',
        'date_of_enrollment',
        'gender', 
        'race',
        'program_location', 
        'program_stream',
        'cohort_id',
        'cohort_name', 
        'cohort_start_date',
        'program_name',
        'first_name',
        'last_name',
        'email',
        'birthdate',
        'external_reference_id'
    ]
    
    cache_file = new_cache_file("sf")
    res.to_csv(cache_file, index = False)
    print(f"Cache file created: {cache_file}")
    print(f"Data fetched from Salesforce.")
    return(res)



def null_attributes(d): 
    if type(d) is not OrderedDict: 
        return 
    elif d.get('attributes'): 
        d.pop('attributes')
        
    for _, v in d.items():
        null_attributes(v)
            
    return 


def sfdc_to_df(res):
    for x in res['records']: 
        null_attributes(x)
        
    return pd.json_normalize(res['records'])
    
extract(fetch = True)