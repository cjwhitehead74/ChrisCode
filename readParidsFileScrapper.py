
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup as bs
from decimal import Decimal
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import os.path
from time import gmtime, strftime, sleep

def stripTest(s):
    return s.text.strip()

def scraper(parid,pFile,sFile,tFile,dFile):    
    
    if indx == 0:
        try:
            os.remove(pFile)
            os.remove(sFile)
            os.remove(tFile)
            os.remove(dFile)
        except OSError:
            pass

    url = "http://delcorealestate.co.delaware.pa.us/PT/Datalets/Datalet.aspx?mode=&UseSearch=no&pin={}&jur=023&taxyr=2021".format(parid)

    r = requests.get(url)

    resOH = []
    resTaxRec = []
    resP1 = []
    resP2 = []
    resP3 = []
    resP4 = []
    resP5 = []
    resP6 = []
    
    rOH = pd.DataFrame()   
    rTR = pd.DataFrame() 
    rP1 = pd.DataFrame() 
    rDT = pd.DataFrame()
    pi2 = pd.DataFrame()   
    pi3 = pd.DataFrame()   
    pi4 = pd.DataFrame()   
    pi5 = pd.DataFrame()
    pi6 = pd.DataFrame()

    if r.status_code == 200:
        
        soup = bs(r.content, "lxml")
        
        test=soup.find("table", attrs={"id": "Owner"})
        
        if test is not None:
            print(str(parid) + " has Data!!!! ")
        
    #sale transactions
            ownHist=soup.find("table", attrs={"id": "Owner History"})
    
            if ownHist is not None:
                for i,row in enumerate(ownHist.find_all('tr')):
                    if i > 0:
                        col = row.find_all('td')
                        if len(col) >= 2:
                            tup = tuple([a.text.strip() for a in col])
                            resOH.append(tup)
    
            OH = pd.DataFrame(resOH,columns=['owner','book','page','saleDate','salePrice'])
            OH["parid"] = parid
            OH["updateDate"] = pd.datetime.now().date()
    
            rOH = OH[['parid','updateDate','owner','book','page','saleDate','salePrice']]
        
            rOH['saleDate'] = pd.to_datetime(rOH['saleDate'], errors = 'coerce').dt.date    
            rOH.set_index('parid',drop=True,inplace=True) 
            rOH = rOH.sort_values(['parid', 'saleDate'], ascending=[True, False])
            rOH['lastSale'] = (rOH.groupby('parid').cumcount() == 0).astype(int)

            file_exists = os.path.isfile(sFile)
            
            if not file_exists:
                rOH.to_csv(sFile, index=True, header=True)
            else:
                rOH.to_csv(sFile, index=True, header=False, mode='a')
    
    
     #tax receivable
            taxRec=soup.find("table", attrs={"id": "County Tax Receivable"})
    
            if taxRec is not None:
                for i,row in enumerate(taxRec.find_all('tr')):
                    if i > 0:
                        col = row.find_all('td')
                        if len(col) >= 2:
                            tup = tuple([a.text.strip() for a in col])
                            resTaxRec.append(tup)
    
            TR = pd.DataFrame(resTaxRec,columns=['tax_year', 'billing_year', 'billing_period', 'billing_date', 'face_amount_due', 'discount_posted', 'penalty_posted', 'fees_posted', 'payment_posted', 'balance', 'pay_date', 'type'])
            TR["parid"] = parid
            TR["updateDate"] = pd.datetime.now().date()
    
            rTR = TR[['parid','updateDate','tax_year', 'billing_year', 'billing_period', 'billing_date', 'face_amount_due', 'discount_posted', 'penalty_posted', 'fees_posted', 'payment_posted', 'balance', 'pay_date', 'type']]
 
            rTR['pay_date'] = pd.to_datetime(rTR['pay_date'], errors = 'coerce').dt.date  
            rTR['billing_date'] = pd.to_datetime(rTR['billing_date'], errors = 'coerce').dt.date  
            rTR.set_index('parid',drop=True,inplace=True)
            rTR = rTR.sort_values(['parid', 'pay_date'], ascending=[True, False])
            rTR['lastPay'] = (rTR.groupby('parid').cumcount() == 0).astype(int)

            file_exists = os.path.isfile(tFile)
            
            if not file_exists:
                rTR.to_csv(tFile, index=True, header=True)
            else:
                rTR.to_csv(tFile, index=True, header=False, mode='a')
   
  
     #Delinquent Tax
            property5=soup.find("table", attrs={"id": "Delinquent Tax - All Years Combined"})
        
            if property5 is not None:
                for i,row in enumerate(property5.find_all('tr')):
                    if i >= 0 :
                        col = row.find_all('td')
                        if len(col) >= 2:
                            tup = list([a.text.strip().strip('.').replace(',','') for a in col])
                            resP5.append(tup)
            
            resP5 = [a for a in resP5 if 'Total Due' in a or 'TOTAL' in a]
                        
            for i,item in enumerate(resP5):
                if i <= 0:
                    for x in range(len(item)):
                        item[x] = item[x].lower().replace(' ','_').replace('.','_')
         
            if property5 is not None:
                rDT = pd.DataFrame(resP5)                       
                new_header = rDT.iloc[0] #grab the first row for the header
                rDT = rDT[1:] #take the data less the header row
                rDT.columns = new_header #set the header row as the df header
                rDT.drop(rDT.columns[[0, 1]], axis = 1, inplace = True)
           
                rDT["parid"] = parid
                rDT["updateDate"] = pd.datetime.now().date()
                rDT['updateDate'] = pd.to_datetime(rDT['updateDate'], errors = 'coerce').dt.date    
                rDT.set_index('parid',drop=True,inplace=True) 
 
                file_exists = os.path.isfile(dFile)
            
                if not file_exists:
                    rDT.to_csv(dFile, index=True, header=True)
                else:
                    rDT.to_csv(dFile, index=True, header=False, mode='a')
        

    
    #Property info
            property1=soup.find("table", attrs={"id": "Parcel"})
        
            if property1 is not None:
                for i,row in enumerate(property1.find_all('tr')):
                    if i >= 0 :
                        col = row.find_all('td')
                        if len(col) >= 2:
                            tup = list([a.text.strip().strip(':').replace("'", "").replace("%", "Pct")  for a in col])
                            resP1.append(tup)
         
            for item in resP1:
                item[0:1] = [item[0].lower().replace(' ','_') for c in item[1:]]
                item[1:2] = [" ".join(item[1].split()) for c in item[1:]]
    
                if resP1[2][0] == '':
                    resP1[2][0] = 'legal_description2'
    
                if resP1[3][0] == '':
                    resP1[3][0] = 'legal_description3'
          
            if property1 is not None:
                pi1 = pd.DataFrame(resP1).transpose()                        
                new_header = pi1.iloc[0] #grab the first row for the header
                pi1 = pi1[1:] #take the data less the header row
                pi1.columns = new_header #set the header row as the df header
                pi1["parid"] = parid
                pi1["updateDate"] = pd.datetime.now().date()
    
                cols = list(pi1.columns)
                cols = [cols[-1]] + cols[:-1]
                cols = [cols[-1]] + cols[:-1]
                rP1 = pi1[cols]
    
    
            #Owner info
            property2=soup.find("table", attrs={"id": "Current Owner"})
        
            if property2 is not None:
                for i,row in enumerate(property2.find_all('tr')):
                    if i >= 0 :
                        col = row.find_all('td')
                        if len(col) >= 2:
                            tup = list([a.text.strip() for a in col])
                            resP2.append(tup)
      
            for item in resP2:
                item[0:1] = [item[0].lower().replace(' ','_') for c in item[1:]]
                item[1:2] = [" ".join(item[1].split()) for c in item[1:]]
    
                if resP2[1][0] == 'name':
                    resP2[1][0] = 'name2'
        
            if property2 is not None:
                pi2 = pd.DataFrame(resP2).transpose()                        
                new_header = pi2.iloc[0] #grab the first row for the header
                pi2 = pi2[1:] #take the data less the header row
                pi2.columns = new_header #set the header row as the df header
      
            rP1 = pd.concat([rP1,pi2], axis=1)
    
    
            #Assessment
            property4=soup.find("table", attrs={"id": "Original Current Year Assessment"})
        
            if property4 is not None:
                for i,row in enumerate(property4.find_all('tr')):
                    if i >= 0 :
                        col = row.find_all('td')
                        if len(col) >= 2:
                            tup = list([a.text.strip().strip('.') for a in col])
                            resP4.append(tup)
      
            for i,item in enumerate(resP4):
                if i <= 0:
                    for x in range(len(item)):
                        item[x] = item[x].lower().replace(' ','_').replace('.','_')
         
            if property4 is not None:
                pi4 = pd.DataFrame(resP4)                       
                new_header = pi4.iloc[0] #grab the first row for the header
                pi4 = pi4[1:] #take the data less the header row
                pi4.columns = new_header #set the header row as the df header
    
            rP1 = pd.concat([rP1,pi4], axis=1)
    
    
            #Tax Sale Status
            property6=soup.find("table", attrs={"id": "Tax Sale Information"})
        
            if property6 is not None:
                for i,row in enumerate(property6.find_all('tr')):
                    if i >= 0 :
                        col = row.find_all('td')
                        if len(col) >= 2:
                            tup = list([a.text.strip().strip(':') for a in col])
                            resP6.append(tup)
            
            if resP6[0][0] == 'Status':
                    resP6[0][0] = 'tax_sale_status'
       
            if property6 is not None:
                pi6 = pd.DataFrame(resP6).transpose()                       
                new_header = pi6.iloc[0] #grab the first row for the header
                pi6 = pi6[1:] #take the data less the header row
                pi6.columns = new_header #set the header row as the df header
     
            rP1 = pd.concat([rP1,pi6], axis=1)
    
            #Mortgage info
            property3=soup.find("table", attrs={"id": "Mortgage Company"})
        
            if property3 is not None:
                for i,row in enumerate(property3.find_all('tr')):
                    if i >= 0 :
                        col = row.find_all('td')
                        if len(col) >= 2:
                            tup = list([a.text.strip().strip('.') for a in col])
                            resP3.append(tup)
      
            for i,item in enumerate(resP3):
                if i <= 0:
                    for x in range(len(item)):
                        item[x] = item[x].lower().replace(' ','_').replace('.','_')
         
            if property3 is not None:
                pi3 = pd.DataFrame(resP3)                       
                new_header = pi3.iloc[0] #grab the first row for the header
                pi3 = pi3[1:] #take the data less the header row
                pi3.columns = new_header #set the header row as the df header
      
            rP1 = pd.concat([rP1,pi3], axis=1)
    
            column_list = ['mortgage_company','mortgage_service_co_name']

            for col in column_list:
                if col not in rP1.columns:
                    rP1[col] = np.nan
            
            rP1['assessment_value'] = rP1['assessment_value'].str.replace(",", "").str.replace("$", "").astype(float)
            rP1.set_index('parid',drop=True,inplace=True) 
            rP1 = rP1.sort_values(['parid'], ascending=[True])
            
            file_exists = os.path.isfile(pFile)
            
            if not file_exists:
                rP1.to_csv(pFile, index=True, header=True)
            else:
                rP1.to_csv(pFile, index=True, header=False, mode='a')
            
            
        else:
          print(str(parid) + " ")
    
        return rP1, rOH, rTR, rDT
 
    

#####################################################################
#####################################################################

masterParids = pd.read_csv('test.csv',header=None)
masterParids.columns =['parid'] 
masterParids['parid'] = masterParids['parid'].str.replace("'","")
mp = sorted(set(masterParids['parid'].tolist()))


# mp = [34000254700, 34000254701, 34000254706, 12000000100] 


for indx, i in enumerate(mp):
    rP1, rOH, rTR, rDT = scraper(i,'propertyInfo_Sample.csv','saleTransactions_Sample.csv','taxReceivable_Sample.csv','delinquentTax_Sample.csv')

    






#####################################################################

# masterParids = pd.read_csv('masterParidsShort.csv',header=None)
# masterParids.columns =['parid'] 
# masterParids['parid'] = masterParids['parid'].str.replace("'","")
# mp = set(masterParids['parid'].tolist())





























