import pandas as pd
import sqlite3
import sqlalchemy
from sqlalchemy import inspect
import re
import datetime as dt

#initialize database with filename myDB in current directory
engine = sqlalchemy.create_engine('sqlite:///myDB.db')
#read in 2 million + rows csv in chunks 
chunksize = 100000
j= 1
i = 0
start = dt.datetime.now()
for df in pd.read_csv("C://Users//SEAB//Downloads//Active_Corporations___Beginning_1800.csv", chunksize = chunksize, iterator = True):
	df = df.rename(columns={c:c.replace(' ', '')  for c in df.columns})
	df.index +=j
	#notice that i is index for testing, in case you want to print which chunk you are at 
	i+=1
	print('{} seconds: completed {} rows'.format((dt.datetime.now() - start).seconds, i*chunksize))
	#activeCo will be the name of your table
	df.to_sql('activeCo', engine, if_exists = 'append')
	#df.index is a range of the rows, so with df.index[-1], we are going to the last row number and adding 1
	j= df.index[-1] + 1

#start connection to the database you created before with the engine
conn = sqlite3.connect('myDB.db')
cur = conn.cursor()

#now we want to add a column to our table which is the punctuation and space stripped version of CurrentEntityName

#first define a python function that we will use to remove spaces and punctuations 
def cleanWord(word):
	removeThe = re.sub(r"The ","",word)
	cleaned = re.sub(r"(?i)\band\b|\bor\b|\bthe\b|\bof\b", "", removeThe)
	myStrippedName = re.sub(r"[.?!'-+&/, ]", "", cleaned)
	return myStrippedName
#now we need to pass this function to our connection to make it sql compatible
#our sql function will have the name cleanWord as well
conn.create_function("cleanWord", 1, cleanWord)
#next line tests that our function works as we thought
#sql  ='SELECT cleanWord(CurrentEntityName) FROM activeCo AS Test LIMIT 3'

#NOTE: Check that the column does not exist first by running
#sql = 'SELECT * FROM sqlite_master'
#you only have to check the last column because sqlite will always add new columns to the end

#Now we can create our column safely
#NOTE: Sqlite does not support DROP COLUMN so be careful with naming 
sql = 'ALTER TABLE activeCo ADD COLUMN CleanedEntityName TEXT;'
cur.execute(sql)
#now we update the values. for me it took 80 seconds. 
start = dt.datetime.now()
sql = 'UPDATE activeCo SET CleanedEntityName = cleanWord(CurrentEntityName)'
cur.execute(sql)
print('{} seconds: completed filling values'.format((dt.datetime.now() - start).seconds))
#now create index for speeding up performance
#NOTE: this is necessary because without index, we have extremely slow query
sql = 'CREATE INDEX CleanedEntityIdx ON activeCo(CleanedEntityName COLLATE NOCASE)'
cur.execute(sql)
#for testing purposes only, confirms that indexing is being used for parameterized search
#params = ('Alpha%',)
# sql = 'EXPLAIN QUERY PLAN SELECT * FROM activeCo WHERE CleanedEntityName LIKE ?;'
#cur.execute(sql, params)
#cur.fetchall()
#you should see [(3, 0, 0, 'SEARCH TABLE activeCo USING INDEX CleanedEntityIdx (CleanedEntityName>? AND CleanedEntityName<?)')]

#now read in the dataset we want to left join into df
my_df = pd.read_csv("C://Users//SEAB//Downloads//M_WBE__LBE__and_EBE_Certified_Business_List.csv")
numberRows = my_df.shape[0]
#list of suffixes that we want to drop
my_list= ['LLC', 'CORP',
          'INC', 'CORPORATION',
          'PLLC', 'PC',
          'LLP', 'CO', 'COMPANY', 'LTD']
myNames = [None]*numberRows
#trim suffixes in our list off of words
for index, row in my_df.iterrows():
        temp1= row['Vendor_Formal_Name'].rsplit(',', 1)
        temp2 = row['Vendor_Formal_Name'].rsplit(' ',1)
        if len(temp1) > 1 and re.sub(r"[,.?!'-+&/ ]", "", temp1[1]).upper() in my_list:
                myNames[index] = temp1[0]
        elif len(temp2) > 1 and re.sub(r"[,.?!'-+&/ ]", "", temp2[1]).upper() in my_list:
                        myNames[index] = temp2[0]
        else:
                myNames[index]=row['Vendor_Formal_Name']
#get our responses. Notice, with indexing this should only take a few seconds, definitely less than a minute.  
myresponses= [None]*numberRows
start = dt.datetime.now()
for index, name in enumerate(myNames):
        sql = 'SELECT * from activeCo WHERE CleanedEntityName LIKE ?;'
        cleanName = cleanWord(name)
        params =(cleanName + '%',)
        response = cur.execute(sql, params)
        tables = response.fetchall()
        myresponses[index] = tables
print('{} seconds: completed getting query results'.format((dt.datetime.now() - start).seconds))

#with this method we can get 7969 results, or 87.3% matching.
#While obviously this is less than ideal, we can consider what possible improvements to be made
#and the tradeoff of complexity of implementation vs. results gained from this

#now let's left join our results to our dataframe. 

#get column names first:
result = conn.execute('SELECT * FROM activeCo LIMIT 1')
names = list(map(lambda x: x[0], result.description))
numberColumns = len(names)
#create new list of tuples for conversion to dataframe 
myNewResponses = [response[0] if response != [] else tuple([None]*numberColumns) for response in myresponses]
new_df = pd.DataFrame(myNewResponses, columns = names)
temp = pd.concat([my_df, new_df], axis = 1)
temp = temp.drop(['CleanedEntityName'], axis = 1)
cols = temp.columns.tolist()
#move CurrentEntityName to the front of table in order for more convenient comparison between matched values
cols.insert(0, cols,pop(cols.index('CurrentEntityName')))
final_df = temp.reindex(columns = cols)
final_df.to_csv("MyJoinedData.csv")
