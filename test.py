import pandas as pd
import numpy as np
import os
from tqdm.notebook import tqdm
from datetime import datetime as dt
from datetime import timedelta

# from __future__ import print_function
import os.path
from googleapiclient.discovery import build
from google.oauth2 import service_account

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = 'keys.json'

credentials = None
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)

SAMPLE_SPREADSHEET_ID = '1Ycg7zTxds9DZnDvTrFcyNNKuTUxg6Yy6WF0a8Wc02WQ'

service = build('sheets', 'v4', credentials=credentials)

spread_sheets = {
    'transcations':"transactions!A1:D29001",
    'clients':"clients!A1:C75767",
    'managers':"managers!A1:C14",
    'leads':"leads!A1:F3338"
}

print('reading from google sheet')

for sheet in spread_sheets:
    result = service.spreadsheets().values().get(
    spreadsheetId=SAMPLE_SPREADSHEET_ID,
    range=spread_sheets[sheet]).execute()

    data = result.get('values', [])

    headers = data.pop(0)
    df = pd.DataFrame(data, columns=headers)

    print(spread_sheets[sheet], df.shape)

    spread_sheets[sheet] = df

clients_df = spread_sheets['clients']
leads_df = spread_sheets['leads']
managers_df = spread_sheets['managers']
transactions_df = spread_sheets['transcations']

leads_df['d_utm_source'] = leads_df['d_utm_source'].replace({
    'vk':'vkontakte',
    'insta': 'instagram'
})
leads_df = leads_df[(leads_df.d_utm_source.str.len() > 1) & (leads_df.d_utm_medium.str.len() > 1)]

leads_df['created_at'] = pd.to_datetime(leads_df['created_at'])
leads_df['ts_created_at'] = leads_df[['created_at']].apply(lambda x: x[0].timestamp(), axis=1).astype(int)

transactions_df['created_at'] = pd.to_datetime(transactions_df['created_at'])
transactions_df['ts_created_at'] = transactions_df[['created_at']].apply(lambda x: x[0].timestamp(), axis=1).astype(int)

leads_df = pd.merge(left=leads_df, right=managers_df, left_on='l_manager_id', right_on='manager_id')
leads_df['is_true_lead'] = leads_df['l_client_id'].isin(clients_df['client_id'].unique().tolist())
leads_df['trash_lead'] = ~leads_df['is_true_lead']
temp = leads_df.groupby('l_client_id')['created_at'].min()
leads_df = leads_df.merge(temp, left_on='l_client_id', right_on='l_client_id', how='left')
leads_df['is_first_lead'] = leads_df['created_at_x'] == leads_df['created_at_y']

transactions_df['m_real_amount'] = transactions_df['m_real_amount'].astype(int)
leads_df['full_new_lead'] = ~leads_df['l_client_id'].isin(transactions_df[transactions_df['m_real_amount'] > 0]['l_client_id'].unique().tolist()) & leads_df['is_first_lead']

for i in range(leads_df.shape[0]):
    lead_row = leads_df.loc[i]

    transac = (transactions_df['l_client_id']==lead_row['l_client_id']) & (transactions_df['created_at'] < lead_row['created_at_x']) & (transactions_df['m_real_amount'] > 0)
    leads_df.loc[i, 'has_transactions'] = transac.max()

    transac = (transactions_df['l_client_id']==lead_row['l_client_id']) & (transactions_df['created_at']>=lead_row['created_at_x']) & (transactions_df['created_at']<=(lead_row['created_at_x'] + timedelta(days=7)))
    leads_df.loc[i, 'is_7day_buyer'] = transac.max()

leads_df['no_transactions'] = ~leads_df['has_transactions']
leads_df['new_client'] = leads_df['is_first_lead'] & leads_df['no_transactions']
leads_df['is_new_7day_buyer'] = leads_df['new_client'] & leads_df['is_7day_buyer']

leads_df = leads_df.merge(transactions_df.groupby(['l_client_id'])['m_real_amount'].sum(), left_on='l_client_id', right_on = 'l_client_id')
leads_df['total_amount'] = leads_df['is_new_7day_buyer'] * leads_df['m_real_amount']

leads_df['is_7day_buyer'] = leads_df['is_7day_buyer'].astype('bool')

print('creating final df')

result_df = leads_df.groupby(['d_utm_medium', 'd_utm_source', 'd_club']).agg(
    count_leads=('lead_id', 'count'),
    count_trash_leads=('trash_lead', 'sum'),
    count_new_leads=('full_new_lead', 'sum'),
    count_buyers=('is_7day_buyer', 'sum'),
    count_new_buyers=('is_new_7day_buyer', 'sum'),
    total_income = ('total_amount', 'sum')
)

result_df = result_df.reset_index()
result = result_df.values.tolist()
result = [list(result_df.columns)] + result

print('writing to google sheet')
result_id = '1ZOaVKG0GooCxeZr2EEeCsjuEgmzGbVNjVgU6uSxc9sg'
request = service.spreadsheets().values().update(
    spreadsheetId=result_id,
    range="final!A1",
    valueInputOption='USER_ENTERED',
    body={
        "values":result
    }
).execute()

print('done!')
# ['lead_id', 'trash_lead', 'full_new_lead', 'is_7day_buyer', 'is_new_7day_buyer'].count()
