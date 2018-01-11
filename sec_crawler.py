# -- coding: utf-8 -*-
"""
Created on Tue Mar  7 11:47:06 2017

@author: daniel, mar, lars (seit dez 17)
"""

from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import time
import datetime
import os
import string
import requests
import re
import ftplib
import numpy as np

#os.chdir('/home/daniel/Documents/python')
#os.chdir('/home/marius/Dropbox/TUD_Gerrit_Marius_Nils_Daniel/Python/SEC')
#os.chdir('/home/lars/Dropbox/Finance/sec_crawler/') #Linux-Rechner
#os.chdir('C:/Users/Lars Poppe/Dropbox/Finance/sec_crawler/') #Windows-Rechner

data_path = 'data/'
output_path = 'output/'

# Get CIK numbers from www.sec.gov (Crawler)
def get_cik_crawler():
    
    # Prepare suffixes
    letters = list(string.ascii_lowercase[:22])
    letters[20], letters[21] = 'uv', 'wxyz'
    letters = ['123'] + letters
    
    # Generate list of URLs
    urls = ['https://www.sec.gov/divisions/corpfin/organization/cfia-'+letter+'.htm' for letter in letters]
    companies_df = pd.DataFrame(columns=['company', 'cik_number', 'sic_code'])
    for url in urls:
        soup = BeautifulSoup(requests.get(url).text, 'lxml')
        
        # Data shown in table, id = "cos" for data containing company information
        rows = str(soup.find_all(id="cos")).split('<tr valign="top">')
        
        re_company = r'\b\>[@A-Z&a-z;,-\. 0-9 \/]{1,100}'
        re_cik = r'\b[0-9]{7}\b'
        re_sic = r'\>[0-9]{1,4}\<'
        

        for row in rows[1:-1]:
            company = re.findall(re_company, row)[0][1:] # remove >
            company = company.replace('&amp;', '&') # replace bad notations

            cik = re.findall(re_cik, row)[0]
            sic = re.findall(re_sic, row)[0][1:-1] # remove > and < String
            print(company, cik, sic)
            
            companies_df = companies_df.append({'company': company, 'cik_number': cik, 'sic_code': sic}, ignore_index=True)
    
    # Write DataFrame to .csv and .xlsx
    filename = data_path+'cik_new'
    companies_df.to_csv(filename+'.csv')
    companies_df.to_excel(filename+'.xlsx')
    print("Company Names, CIK and SIC received. Written to %s.xlsx and %s.csv" % (filename, filename))
    
    return companies_df


# Get old CIK numbers from cik_OLD.txt
def get_cik_old():
    file = data_path+'cik_OLD.txt'
    
    re_cik = r'[0-9]{10}' # CIK-Numbers have exact length of 10 digits (cut zeros of later)
    
    with open(file) as f:
        # Read file and get CIK-Numbers + Index of String (positions)
        contents = f.read()
        cik_numbers = re.findall(re_cik, contents)
        positions = [0] + [m.start(0) for m in re.finditer(re_cik, contents)] # add 0 manually
        
        # create DataFrame
        data = pd.DataFrame()
        
        companies = []
        
        for i in np.arange(len(cik_numbers)):
            if i == 0: # i = 0 manually
                company_name = contents[positions[0]:positions[1]-1]
                companies.append(company_name)
                print(company_name)
                
            elif i < len(cik_numbers)-1:
                start = positions[i] + 10 + 2 # plus CIK number plus ': \n'
                end = positions[i+1] - 1 # minus ':'
                company_name = contents[start:end]
                companies.append(company_name)
                print(company_name)
            
            else: # add last item manually 
                company_name = contents[positions[i] + 12 : positions[i+1] - 1]
                companies.append(company_name)
                print(company_name)
                
        data = pd.DataFrame({'company': companies, 'cik_number': cik_numbers})
        
        # Write to .csv
        filename = 'cik_OLD.csv'
        data.to_csv(data_path+filename)
        
        return data
    
def load_data():
    # Load new + old data from .csv-files. Combine data to DataFrame and store local. Load Data from local .csv if True
    filename = 'full_data.csv'
    
    data_old = pd.read_csv(data_path+'cik_OLD.csv', index_col=0)
    data_new = pd.read_csv(data_path+'cik_new.csv', index_col=0)
    
    # Manipulate old/new DataFrames
    d1 = data_new.drop('sic_code', 1)
    d2 = data_old.reindex_axis(['company', 'cik_number'], axis=1)
    full_data = pd.concat([d1, d2], ignore_index=True)

    full_data.to_csv(data_path+filename)
    
    return full_data

def find_start():
    # Find latest cik to continue script 
    ciks_file = output_path+'ciks_used.csv'
    if os.path.isfile(ciks_file) == True and os.stat(ciks_file).st_size != 0:
        print('Test')
        latest_csv = pd.read_csv(output_path+'ciks_used.csv')
        starting_point = latest_csv.index[len(latest_csv)-1]
    else:
        starting_point = 0
            
    return starting_point


def get_infos(entry):
    items = str(entry.find('summary').get_text())
    if 'Item 3:' in items:
        acc_num = str(entry.find('accession-nunber').get_text())
        filing_date = str(entry.find('filing-date').get_text())
        filing_href = str(entry.find('filing-href').get_text())

        return([filing_date, filing_href, acc_num])
            
    elif 'Item 1.03:' in items:
        acc_num = str(entry.find('accession-nunber').get_text())
        filing_date = str(entry.find('filing-date').get_text())
        filing_href = str(entry.find('filing-href').get_text())

        return([filing_date, filing_href, acc_num])

    else:
        return('-1')

def go_through_sites(cik, start = 100):
    url2 = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK='+cik+'&type=8-K%25&dateb=&owner=include&start='+str(start)+'&count=100&output=atom'
    page = urlopen(url2).read()
    soup = BeautifulSoup(page,"lxml")
    entries = soup.find_all('entry')
    if len(entries) > 0:
        return(True)
    else:
        return(False)


def site_exists(cik):
    cik = str(cik)
    
    try: 
        url2 = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK='+cik+'&type=8-K%25&dateb=&owner=include&start=0&count=100&output=atom'
        page = urlopen(url2).read()
        soup = BeautifulSoup(page,"lxml")
    
        if str(soup.find('h1').get_text()) == "No matching CIK.":
            return(False)
        else:
            return(True)
    except:
        return(True)

        
def find_all_defs(cik):
    cik = str(cik)
    chaps11 = []

    try:

        url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK='+cik+'&type=8-K&dateb=&owner=exclude&count=100'
        page = urlopen(url).read()
        soup = BeautifulSoup(page,"lxml")
        cName = str(soup.find_all('span', {'class' : 'companyName'})[0].get_text().split(' CIK')[0])
        cSIC = int(str(soup.find_all('p', {'class' : 'identInfo'})[0].get_text().split('SIC: ')[1].split(' - ')[0]))
    
        start = 0
        while go_through_sites(cik, start):
            url2 = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK='+cik+'&type=8-K%25&dateb=&owner=include&start='+str(start)+'&count=100&output=atom'    
            page = urlopen(url2).read()
            soup = BeautifulSoup(page,"lxml")

            entries = soup.find_all('entry')

            for entry in entries:
                chaps11.append(get_infos(entry))
    
            start += 100
        
        chaps11 = [[cik, cName, cSIC] + chap for chap in chaps11 if chap != '-1']
    
        
    except:
        chaps11 = []
    
    return(chaps11)

# Load Data (old + new)
data = load_data()
data = data.dropna(axis=0) # remove Rows with Company NaN or CIK NaN
counter = 0
save = True
save_period = 2
defaults = []
ciks_used_list = []

# Loop through cik_numbers of data_new with starting point (continue script at latest index saved)
start = find_start()
print("Progress so far:", start, " / ", len(data))
data_ = data['cik_number'][start:]

for i, cik in enumerate(data_):
    
    ciks_used_list.append(cik) 
    
    if counter % 1000 == 0:
        print(datetime.datetime.now())
    counter += 1
    time.sleep(0.05)    
    
    if site_exists(cik):
        print("Currently working on: \n", data.ix[i+start])
        
        tmp = find_all_defs(cik)
        #print tmp
        if len(tmp) > 0 :
            print("Found Default!")
            [defaults.append(tmp2) for tmp2 in tmp]
    

        
   
    # Save Data every 10 minutes to avoid data loss
    if datetime.datetime.now().minute % save_period == 0 and save:
        
        df_defaults = pd.DataFrame(defaults)
        df_ciks_used = pd.DataFrame(ciks_used_list)
        
        # To avoid error
        try:
            df_defaults.columns = ['cik', 'cName', 'sic', 'filing_date', 'filing_url', 'accession_num']
        except:
            print('No defaults yet')
        
        # Log tbc 
        now = datetime.datetime.now()
        
        # Append to .csv
        with open(output_path+'defaults_sec.csv', 'a') as f:
            df_defaults.to_csv(f, header=False, index=False) # just rows without Index
            f.close()
            
        with open(output_path+'ciks_used.csv', 'a') as f:
            df_ciks_used.to_csv(f, header=False, index=False)        
            f.close()
            
        print("Saved: defaults, ciks_used.")
        
        ciks_used_list = []
        defaults = []
        
        print("Cleared ciks_used and defaults list.")
        save = False
        
    if datetime.datetime.now().minute % save_period != 0:
        save = True
          
# Upload data to FTP (wp.firrm.de)
# upload_file.py
