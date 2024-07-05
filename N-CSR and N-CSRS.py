import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import numpy as np
from datetime import datetime
import re

CIKs = ['0001678124', '0001803498', '0001842754', '0001736035', '0001061630', '0001735964']

pd.options.display.float_format = '{:.0f}'.format

finaldf = pd.DataFrame() # Initialize an empty DataFrame to store the final results
for cik in CIKs:

    # Define the headers for the HTTP request
    header = {
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding':'gzip, deflate, br, zstd',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
    }

    # Make a request to the SEC website to get JSON data for the given CIK filings
    resp = requests.get(f'https://data.sec.gov/submissions/CIK{cik}.json', headers=header)
    data = resp.json()['filings']['recent']  # Extract recent filings from the JSON response
    temp_df = pd.DataFrame(data)  # Convert the data to a DataFrame

    # Convert filingDate to datetime and filter for filings after 2019-01-01
    temp_df['filingDate'] = pd.to_datetime(temp_df['filingDate'], format='%Y-%m-%d')
    temp_df = temp_df[temp_df['filingDate'] > '2019-01-01']
    temp_df['filingDate'] = temp_df['filingDate'].dt.strftime('%m/%d/%Y')

    # Convert reportDate to datetime and format it
    temp_df['reportDate'] = pd.to_datetime(temp_df['reportDate'])
    temp_df['reportDate'] = temp_df['reportDate'].dt.strftime('%m/%d/%Y')

    # Filter for N-CSR and N-CSRS form
    temp_df = temp_df[(temp_df['form']=='N-CSR') | (temp_df['form']=='N-CSRS')]

    # Convert DataFrame to JSON
    submissions_json = json.loads(temp_df.to_json(orient='records'))
    #Loop through the JSON data
    for submission in submissions_json:
        # Extract details from the submission
        accessionNumber = submission['accessionNumber']
        accessionNumber_no_dashes = submission['accessionNumber'].replace('-','')
        filingDate = submission['filingDate']
        isXBRL = submission['isXBRL']
        isInlineXBRL = submission['isInlineXBRL']
        form = submission['form']
        
        # Define headers for the HTTP request to get the filing document
        headers={
            "Accept-Encoding": "gzip, deflate",
            "User-Agent": 'MyCompanyName my.email@domain.com',
            "Host": 'www.sec.gov',
        }
        resp = requests.get(f'https://www.sec.gov/Archives/edgar/data/{cik[3:]}/{accessionNumber_no_dashes}/{accessionNumber}.txt', headers=headers)
        soup = BeautifulSoup(resp.content, 'lxml').find('body') # Parse the document with BeautifulSoup
        df = None
        for i in soup.find_all('table'):
            # Find the table with 'Assets' or 'Assets:' as the first row
            if i.find_all('tr')[0].text.strip() == 'Assets':
                df = pd.read_html(StringIO(str(i)))[0]
                break
            elif i.find_all('tr')[0].text.strip() == 'Assets:':
                df = pd.read_html(StringIO(str(i)))[0]
                break

        # Extract the Company data from the soup object
        compnayData = {}
        for i in soup.find('acceptance-datetime').text.split('\n'):
            if i:
                try:
                    values = i.split(':')
                    compnayData[values[0].strip()] = values[1].strip()
                except:pass
        # Extract FundId from the Company data
        FundId = compnayData['COMPANY CONFORMED NAME']
        try:
            # Extract and format the report date
            reportDate = compnayData['CONFORMED PERIOD OF REPORT']
            reportDate = datetime.strptime(reportDate, '%Y%m%d').strftime('%m/%d/%Y')
        except:
            # Extract and format the report date from the soup object if not found in company data
            reportDate = soup.find_all(lambda tag: tag.name == 'p' and 'Date of reporting period:' in tag.text)[0].text.split(':')[-1].split('–')[-1].split('-')[-1].strip()
            reportDate = datetime.strptime(reportDate, '%B %d, %Y').strftime('%m/%d/%Y')
        
        # Clean and prepare the DataFrame
        df = df.dropna(axis=1, how='all')
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)
        df = df.fillna('')
        df = df.T.drop_duplicates().T
        try:
            df.columns = ['FactID', 'sign', 'ValueNum']
        except:
            try:
                df['merged'] = df.iloc[:, 2] + ' ' + df.iloc[:, 3] + ' ' + df.iloc[:, 4].astype(str)
            except:
                df['merged'] = df.iloc[:, 2] + ' ' + df.iloc[:, 3].astype(str)
            df = df.iloc[:, [0, -1]]
            df.columns = ['FactID', 'ValueNum']
        
        # Add additional columns to the DataFrame
        df['reportDate'] = reportDate
        df['EndDate'] = reportDate
        df['Period'] = df['EndDate']
        df['FundID'] = FundId
        df ['CIK'] = cik
        df['Measure'] = 'USD'
        df['FormCode'] = form
        df['FactTag'] = 'Us-gaap:'+df['FactID']
        df['StartDate'] = ''
        
        df['PeriodFY'] = df['Period'].str.split('/').str[-1]
        finaldf = pd.concat([finaldf, df])
        df = None
        try:
            # Attempt to find and process the second table (Consolidated Statements of Changes in Net Assets) in the document
            df = pd.read_html(StringIO(str(soup.find_all(lambda tag: tag.name == 'td' and "Net Increase in Net Assets from:" in tag.text)[-1].findParent('table'))))[0]
            df = df.dropna(axis=1, how='all')

            # Clean and prepare the DataFrame
            df.columns = df.iloc[0]
            df = df[1:].reset_index(drop=True)
            df = df.fillna('')
            df = df.rename(columns={np.nan: 'FactID'})
            df_merged = df.T.groupby(level=0).sum().T
            temp_columns = [i.strip().replace('\xa0',' ').split('Ended')[-1].split('through')[-1].replace('*','').replace('(Unaudited)','').strip() for i in df_merged.columns.to_list()]
            columns = []
            for column in temp_columns:
                try:
                    Date = datetime.strptime(column, '%B %d, %Y').strftime('%m/%d/%Y')
                    columns.append(Date)
                except:
                    columns.append(column)
            df_merged.columns = columns
            df_merged['ValueNum'] = df_merged[reportDate]
            df = df_merged[['FactID', 'ValueNum']].copy()
            df['reportDate'] = reportDate
            df['EndDate'] = reportDate
            df['Period'] = df['EndDate']
            df['FundID'] = FundId
            df ['CIK'] = cik
            df['Measure'] = 'USD'
            df['FormCode'] = form
            df['FactTag'] = 'Us-gaap:'+df['FactID']
            df['StartDate'] = ''
            
            df['PeriodFY'] = df['Period'].str.split('/').str[-1]
            finaldf = pd.concat([finaldf, df])
        except:pass
        
        for table_index in range(1,2):
            # Attempt to find and process the third table (Consolidated Financial Highlights OR Financial Highlights) in the document
            try:
                df = None
                try:
                    df = pd.read_html(StringIO(str(soup.find_all(lambda tag: tag.name == 'p' and "Consolidated Financial Highlights" in tag.text)[-table_index].find_next('table'))))[0]
                except:
                    df = pd.read_html(StringIO(str(soup.find_all(lambda tag: tag.name == 'p' and "Financial Highlights" in tag.text)[-table_index].find_next('table'))))[0]
                
                # Clean and prepare the DataFrame
                df = df.dropna(axis=1, how='all')
                df.columns = df.iloc[0]
                df = df[1:].reset_index(drop=True)
                df = df.fillna('')
                df = df.rename(columns={np.nan: 'temp'})
                columns = []
                for index, i in enumerate(df.columns.to_list()):
                    if i == 'temp' and index != 0:
                        columns.append(df.columns.to_list()[index-1])
                    else:
                        columns.append(df.columns.to_list()[index].strip())
                df.columns = columns
                df = df.T.groupby(level=0).sum().T
                df = df.rename(columns={'temp': 'FactID'})
                temp_columns = [i.strip().replace('\xa0',' ').replace('\u200b','').split('Ended')[-1].split('through')[-1].replace('*','').replace('(Unaudited)','').strip() for i in df.columns.to_list()]
                columns = []
                for column in temp_columns:
                    try:
                        Date = datetime.strptime(column, '%B %d, %Y').strftime('%m/%d/%Y')
                        columns.append(Date)
                    except:
                        columns.append(column.strip())
                df.columns = columns

                df['ValueNum'] = df[reportDate]
                try:
                    df = df[['FactID', 'ValueNum']].copy()
                except:
                    df = df.rename(columns={'Supplemental Expense Ratios': 'FactID'})
                    df = df[['FactID', 'ValueNum']].copy()
                df['reportDate'] = reportDate
                df['EndDate'] = reportDate
                df['Period'] = df['EndDate']
                df['FundID'] = FundId
                df ['CIK'] = cik
                df['Measure'] = 'USD'
                df['FormCode'] = form
                df['FactTag'] = 'Us-gaap:'+df['FactID']
                df['StartDate'] = ''
                
                df['PeriodFY'] = df['Period'].str.split('/').str[-1]
                finaldf = pd.concat([finaldf, df])
                break
            except Exception as e:
                pass

# Filter out rows where FactID is not empty
finaldf = finaldf[finaldf['FactID'] != '']
# add the PeriodFP column Conditionally
finaldf['PeriodFP'] = finaldf['FormCode'].apply(lambda x: 'FY' if x == 'N-CSR' else 'SA')
# Clean the ValueNum column
finaldf.loc[:, 'ValueNum'] = finaldf['ValueNum'].astype(str).str.replace('$', '', regex=False)

df = finaldf[['CIK', 'FundID', 'FactID', 'FactTag', 'Measure', 'ValueNum', 'FormCode','Period', 'reportDate', 'PeriodFY', 'PeriodFP', 'StartDate', 'EndDate']]

df.to_excel('N-CSR and N-CSRS.xlsx',index=False)
