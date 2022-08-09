# -*- coding: utf-8 -*-
"""
Created on Sat Aug  6 18:24:22 2022

@author: Ali
"""

import csv
from io import BytesIO, StringIO
import psycopg2
from sqlalchemy import create_engine
from airflow.models import DAG, TaskInstance
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.dates import days_ago
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup


def download_file(task_instance: TaskInstance, **kwargs):
    sftp_hook = SSHHook(ssh_conn_id="my_sftp_server")
    sftp_client = sftp_hook.get_conn().open_sftp()
    fp = BytesIO()
    input_path = kwargs["templates_dict"]["input_path"]
    sftp_client.getfo(input_path, fp)
    task_instance.xcom_push(key="raw_input_file", value=fp.getvalue().decode("utf-8"))
    
    

def process_file(task_instance: TaskInstance, **kwargs):
    raw_input_file = task_instance.xcom_pull(task_ids="download_file", key="raw_input_file")
   
    df = pd.DataFrame(csv.reader(StringIO(raw_input_file)),columns = ['Date','Valid from','Product number','Product','Country of origin iso','Sales'])
    df = df.iloc[1:,:]
    df['Date'] = pd.to_datetime(df['Date'],format = '%Y%m')
    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    df['Autumn'] = np.where(df['Month'].isin([9,10]), True, False)
    df['Season'] = np.where(df['Month'].isin([5,6,7,8,9,10]),'Summer','Winter')
    df['Season Distinct'] = np.where(df['Month'] < 6,'Winter '+(df['Year'].astype('int')-1).astype('str')+'/'+df['Year'].astype('str'),'Winter '+df['Year'].astype('str')+'/'+(df['Year'].astype('int')+1).astype('str'))
    page = requests.get('https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes').text
    soup = BeautifulSoup(page, 'html.parser')
    table = soup.find('table')
    df1 = pd.read_html(str(table))
    df1 = pd.concat(df1)
    df1.columns = df1.columns.droplevel()
    df1.rename(columns = {'Alpha-2 code[5]':'Country of origin (ISO)','Country name[5]':'Country of origin'},inplace = True)
    df1 = df1[['Country of origin','Country of origin (ISO)']]
    df1.loc[df1['Country of origin'] == 'Afghanistan', 'Country of origin (ISO)'] = "AF"
    df1.loc[df1['Country of origin (ISO)'] == 'GB', 'Country of origin'] = "Great Britain"
    df1.loc[df1['Country of origin (ISO)'] == 'EH', 'Country of origin'] = "Western Sahara"
    df1.loc[df1['Country of origin (ISO)'] == 'AQ', 'Country of origin'] = "Antartica"
    df1.loc[df1['Country of origin (ISO)'] == 'AU', 'Country of origin'] = "Australia"
    df1.loc[df1['Country of origin (ISO)'] == 'BS', 'Country of origin'] = "Bahamas"
    df1.loc[df1['Country of origin (ISO)'] == 'BO', 'Country of origin'] = "Bolivia"
    df1.loc[df1['Country of origin (ISO)'] == 'IO', 'Country of origin'] = "British Indian Ocean Territory"
    df1.loc[df1['Country of origin (ISO)'] == 'BN', 'Country of origin'] = "Brunei Darussalam"
    df1.loc[df1['Country of origin (ISO)'] == 'CV', 'Country of origin'] = "Cabo Verde"
    df1.loc[df1['Country of origin (ISO)'] == 'KY', 'Country of origin'] = "Cayman Islands"
    df1.loc[df1['Country of origin (ISO)'] == 'CF', 'Country of origin'] = "Central African Republic"
    df1.loc[df1['Country of origin (ISO)'] == 'KM', 'Country of origin'] = "Comoros"
    df1.loc[df1['Country of origin (ISO)'] == 'CG', 'Country of origin'] = "Congo"
    df1.loc[df1['Country of origin (ISO)'] == 'CI', 'Country of origin'] = "Côte d'Ivoire"
    df1.loc[df1['Country of origin (ISO)'] == 'CZ', 'Country of origin'] = "Czech Republic"
    df1.loc[df1['Country of origin (ISO)'] == 'DO', 'Country of origin'] = "Dominican Republic"
    df1.loc[df1['Country of origin (ISO)'] == 'SZ', 'Country of origin'] = "Eswatini"
    df1.loc[df1['Country of origin (ISO)'] == 'FO', 'Country of origin'] = "Faroe Islands"
    df1.loc[df1['Country of origin (ISO)'] == 'FR', 'Country of origin'] = "France"
    df1.loc[df1['Country of origin (ISO)'] == 'IR', 'Country of origin'] = "Iran"
    df1.loc[df1['Country of origin (ISO)'] == 'MK', 'Country of origin'] = "North Macedonia"
    df1.loc[df1['Country of origin (ISO)'] == 'MO', 'Country of origin'] = "Macao"
    df1.loc[df1['Country of origin (ISO)'] == 'MM', 'Country of origin'] = "Myanmar"
    df1.loc[df1['Country of origin (ISO)'] == 'NL', 'Country of origin'] = "Netherlands"
    df1.loc[df1['Country of origin (ISO)'] == 'PS', 'Country of origin'] = "Palestine"
    df1.loc[df1['Country of origin (ISO)'] == 'RU', 'Country of origin'] = "Russia"
    df1.loc[df1['Country of origin (ISO)'] == 'SD', 'Country of origin'] = "Sudan" 
    df1.loc[df1['Country of origin (ISO)'] == 'TW', 'Country of origin'] = "Taiwan"     
    df1.loc[df1['Country of origin (ISO)'] == 'TC', 'Country of origin'] = "Turks and Caicos Islands"
    df1.loc[df1['Country of origin (ISO)'] == 'AE', 'Country of origin'] = "United Arab Emirates"     
    df1.loc[df1['Country of origin (ISO)'] == 'US', 'Country of origin'] = "United States of America" 
    df1.loc[df1['Country of origin (ISO)'] == 'VE', 'Country of origin'] = "Venezuela" 
    df1.loc[df1['Country of origin (ISO)'] == 'EH', 'Country of origin'] = "Western Sahara" 
    df1.loc[df1['Country of origin (ISO)'] == 'VN', 'Country of origin'] = "Vietnam"
    df1.loc[df1['Country of origin (ISO)'] == 'KR', 'Country of origin'] = "Republic of Korea" 
    df1.loc[df1['Country of origin (ISO)'] == 'PH', 'Country of origin'] = "Philippines"
    df1.loc[df1['Country of origin (ISO)'] == 'HK', 'Country of origin'] = "Hong Kong"
    df1.loc[df1['Country of origin (ISO)'] == 'SA', 'Country of origin'] = "Saudiarabia"
    df_final = df.merge(df1,left_on = 'Country of origin iso',right_on = 'Country of origin (ISO)',how = 'left')
    conditions = [
    (df_final['Country of origin'] == 'Poland' )
,(df_final['Country of origin'] == 'Portugal') 
,(df_final['Country of origin'] == 'Spain' )
,(df_final['Country of origin'] == 'Switzerland') 
,(df_final['Country of origin'] == 'Netherlands' )
,(df_final['Country of origin'] == 'Belgium' )
,(df_final['Country of origin'] == 'Luxembourg' )
,(df_final['Country of origin'] == 'India' )
,(df_final['Country of origin'] == 'Indonesia' ) 
,(df_final['Country of origin'] == 'Singapore' )
,(df_final['Country of origin'] == 'Malaysia' )
,(df_final['Country of origin'] == 'Thailand' )
,(df_final['Country of origin'] == 'Laos' )
,(df_final['Country of origin'] == 'Russia' )
,(df_final['Country of origin'] == 'China' )
,(df_final['Country of origin'] == 'Taiwan' )
,(df_final['Country of origin'] == 'Hong Kong' ) 
,(df_final['Country of origin'] == 'Japan' )
,(df_final['Country of origin'] == 'Germany' ) 
,(df_final['Country of origin'] == 'Italy' )
,(df_final['Country of origin'] == 'Spain' )
,(df_final['Country of origin'] == 'France' )
,(df_final['Country of origin'] == 'Finland' )
,(df_final['Country of origin'] == 'Denmark' )
,(df_final['Country of origin'] == 'Norway' )
,(df_final['Country of origin'] == 'Sweden' )
,(df_final['Country of origin'] == 'Great Britain' )
,(df_final['Country of origin'] == 'Scotland' )
,(df_final['Country of origin'] == 'Wales' ) 
,(df_final['Country of origin'] == 'United States of America' )
,(df_final['Country of origin'] == 'Canada' )
,(df_final['Country of origin'] == 'Brazil' )
,(df_final['Country of origin'] == 'Czech Republic' ) 
,(df_final['Country of origin'] == 'Republic of Korea' )
,(df_final['Country of origin'] == 'Austria' ) 
,(df_final['Country of origin'] == 'Australia' ) 
,(df_final['Country of origin'] == 'New Zealand' ) 
,(df_final['Country of origin'] == 'Kuwait' )
,(df_final['Country of origin'] == 'Saudiarabia' ) 
,(df_final['Country of origin'] == 'Bahrain' )
,(df_final['Country of origin'] == 'Oman' )
,((df_final['Country of origin'] == 'Katar') | (df_final['Country of origin'] == 'Qatar') )
,(df_final['Country of origin'] == 'United Arab Emirates')]
    choices = [ 
 'Poland'
,'Portugal'
,'Spain'
,'Switzerland'
,'Benelux'
,'Benelux'
,'Benelux'
,'India'
,'South East Asia'
,'South East Asia'
,'South East Asia'
,'South East Asia'
,'South East Asia'
,'Russia'
,'Greater China'
,'Greater China'
,'Greater China'
,'Japan'
,'Germany'
,'Italy'
,'SPain'
,'France'
,'Nordics'
,'Nordics'
,'Nordics'
,'Nordics'
,'United Kingdom'
,'United Kingdom'
,'United kingdom'
,'North America'
,'North america'
,'Brazil'
,'Czech Republic'
,'Republic of Korea'
,'Austria'
,'Australia NZ OC'
,'Australia NZ OC'
,'Gulf Countries'
,'Gulf Countries'
,'Gulf Countries'
,'Gulf Countries'
,'Gulf Countries'
,'Gulf Countries'
]
    df_final['St Market'] = np.select(conditions,choices,default='Other (non-ST market)')
    df_final['St Market 2022'] = df_final['St Market']
    df_final.loc[df_final['Country of origin'] == 'United States of America', 'St Market 2022'] = "United States"
    df_final.loc[df_final['Country of origin'] == 'Canada', 'St Market 2022'] = "Canada"
    df_final.drop('Country of origin (ISO)',axis = 1, inplace = True)
    df_final = df_final.reset_index()  # make sure indexes pair with number of rows
    df_final = df_final.iloc[:,1:]
 
    engine = create_engine('postgresql://postgres:{password}@{host.docker.internal if local host otherwise give ip address of host}:{port}/{name of database}')
    df_final.to_sql('{name of table}', engine,if_exists='append',index=False)
    
    fp = StringIO()
    df_final.to_csv(fp)

    task_instance.xcom_push(key="raw_processed_file", value=fp.getvalue())






with DAG(dag_id = 'trigger_to_process_STS',
         schedule_interval=None,
         start_date=days_ago(2)) as dag:

    sensor = SFTPSensor(task_id="check-for-file",
                        sftp_conn_id="my_sftp_server",
                        path="{/path}",
                        poke_interval=10)

    download_file = PythonOperator(task_id="download_file",
                                   python_callable=download_file,
                                   templates_dict={
                                       "input_path": "{/path}",
                                   })
    process_file = PythonOperator(task_id="process_file",
                                  python_callable=process_file)

    

    sensor >> download_file >> process_file 