#!/usr/bin/env python
# coding: utf-8

# ### Part 1

# In[1]:


tableCommand = {'User': '''
CREATE TABLE User (
    ID               NUMBER(35),
    NAME             VARCHAR2(10000),
    SCREEN_NAME      VARCHAR2(1000),
    DESCRIPTION      VARCHAR2(100000),
    FRIENDS_COUNT    NUMBER(35),
  
    CONSTRAINT User_PK
        PRIMARY KEY(ID)
);''',
                'Geo':'''
CREATE TABLE Geo (
    GEO_ID       NUMBER(35),
    TYPE         VARCHAR2(100),
    LONGITUDE    DECIMAL(9,6),
    LATITUDE     DECIMAL(9,6),
    
    CONSTRAINT Geo_PK
        PRIMARY KEY(Geo_ID)
);''',
                'Tweet':'''
CREATE TABLE Tweet (
    CREATED_AT                 VARCHAR2(35),
    ID_STR                     VARCHAR2(35),
    TEXT                       VARCHAR2(1000),
    SOURCE                     VARCHAR2(500),
    IN_REPLY_TO_USER_ID        NUMBER(35),
    IN_REPLY_TO_SCREEN_NAME    VARCHAR2(200),
    IN_REPLY_TO_STATUS_ID      NUMBER(35),
    RETWEET_COUNT              NUMBER(50),
    CONTRIBUTORS               VARCHAR2(1000),
    USER_ID                    NUMBER(35), 
    GEO_ID                     NUMBER(35), 
  
    CONSTRAINT Tweet_FK
        FOREIGN KEY(USER_ID)
        REFERENCES User(ID),
    CONSTRAINT Geo_FK
        FOREIGN KEY(GEO_ID)
        REFERENCES Geo(GEO_ID));'''}


# In[2]:


import sqlite3
import os
import urllib.request
import json

def text_populateSQL(tableCommands, tableInfo, dataURL):
    '''Takes data URL, table commands, table info (name and attributes), and data URL. 
    Create corresponding tables and populate them with input data.'''
    
    connection = sqlite3.connect('DSC450-Assignment9-Part1.db')
    cursor = connection.cursor()
    
    urlData = urllib.request.urlopen(dataURL)
    dataString = urlData.read().decode('utf-8')

    tweetData = []
    errorTweet = []
    
    #Read data.
    for line in dataString.strip().split('\n'):        
        try: 
            tweet_json = json.loads(line.strip())
            tweetData.append(tweet_json)
        except json.JSONDecodeError:
            errorTweet.append(line)    
    
    print(f"Number of tweets retrieve: {len(tweetData)}")
    
    #Iterate and populate each table.
    for tableName, tableCommand in tableCommands.items():
        cursor.execute(f"DROP TABLE IF EXISTS {tableName}")
        connection.commit()
        cursor.execute(tableCommand)
        
        #Get placeholders for insert statements. 
        attributeName = tableInfo[tableName]
        attributeNum = len(attributeName)
        placeholder = ','.join(['?'] * attributeNum) 
        
        for tweet in tweetData: #Iterate through each tweet.
            try:
                tweetValues = [] #List to store insert values.
                retrievedAttributes = set() #Set to make sure no duplicate attribute retrieve. 
                
                for attribute in attributeName:
                    noncap_tableName = tableName.lower()
                    nestAttribute = tweet.get(noncap_tableName, {}) #Retrieve nested dictionary.
                    if noncap_tableName == 'geo' and nestAttribute is None: 
                    #To handle 'geo' table values where geo dictionary = null.
                        tweetValues.append(None) 
                    elif nestAttribute is not None:
                        if attribute == 'user_id': #Handle 'user_id'. 
                            special_nestAttribute = tweet.get('user', {})
                            tweetValues.append(special_nestAttribute['id']) #Retrieve 'user_id' with user nested dict.
                            retrievedAttributes.add(attribute) 
                        elif attribute in nestAttribute:
                            tweetValues.append(nestAttribute[attribute]) #To handle 'user' table.
                            retrievedAttributes.add(attribute)
                        elif noncap_tableName == 'geo': #To handle 'geo' table, specifically 'geo_id'.
                            coordinates = nestAttribute.get('coordinates', [])
                            geoValues = (nestAttribute['type'], coordinates[0], coordinates[1])
                            cursor.execute("INSERT OR IGNORE INTO geo (type, longitude, latitude) VALUES (?, ?, ?)", geoValues)
                            connection.commit()
                            cursor.execute("SELECT geo_id FROM geo WHERE type = ? AND longitude = ? AND latitude = ?", geoValues)
                            geoID = cursor.fetchone()[0] #Retrieve geo_id as unique identifier of each entry. 
                            tweetValues.append(geoID)  
                        elif attribute in tweet and attribute not in retrievedAttributes:
                            tweetValues.append(tweet[attribute]) #To handle 'tweet' table. 
                    else: 
                        tweetValues.append(None) #If attribute doesn't exists in tweet data.
                
                if tableName == 'Tweet': 
                    tweetValues.append(geoID) #Attach 'geo_id' to last column. 
                tweetValues = tuple(tweetValues) #Convert to tuple for insert statement. 
                cursor.execute(f'INSERT OR IGNORE INTO {tableName} VALUES ({placeholder})', tweetValues)
                
            except ValueError:
                errorTweet.append(json.dumps(tweet)) #Store errors tweet. 
    
    #Write error tweets to text file.
    error_textFile = 'Module9_errors.txt'
    with open(error_textFile, 'w', encoding='utf-8') as infile:
        for error in errorTweet:
            infile.write(error + '\n')
    
    connection.commit()
    connection.close()

    return "Successfully created tables and inserted data."


# In[3]:


textURL = 'https://dbgroup.cdm.depaul.edu/DSC450/Module7.txt'
tableInfo = {'User':["id", "name", "screen_name", "description", "friends_count"], 
             'Geo': ["geo_id", "type", "longitude", "latitude"],
             'Tweet': ["created_at", "id_str", "text", "source", "in_reply_to_user_id", 
                       "in_reply_to_screen_name", "in_reply_to_status_id", 
                       "retweet_count", "contributors", "user_id", "geo_id"]}
text_populateSQL(tableCommand, tableInfo , textURL)


# #### a) Write and execute a SQL query to do the following: Find tweets where tweet id_str contains “89” or “78” anywhere in the column. Time and report the runtime of your query.

# In[4]:


import time

startTime = time.time()

connection = sqlite3.connect('DSC450-Assignment9-Part1.db')
cursor = connection.cursor()
cursor.execute("SELECT * FROM TWEET WHERE id_str LIKE '%89%' OR id_str LIKE '%78%';")
results_A = cursor.fetchall()

endTime = time.time()
runTime_A = endTime - startTime
print(f"Query runtime: {runTime_A:4f} seconds")
print(results_A[:10])


# #### b) Write the equivalent of the previous query in python (without using SQL) by reading it from the file. Time and report the runtime of your query. 

# In[5]:


urlData = urllib.request.urlopen("https://dbgroup.cdm.depaul.edu/DSC450/Module7.txt")
dataString = urlData.read().decode('utf-8')
    
tweetData = []
for line in dataString.strip().split('\n'):        
    try: 
        tweet_json = json.loads(line.strip())
        tweetData.append(tweet_json)
    except json.JSONDecodeError:
        continue    

startTime = time.time()
tweetResults = []
for tweet in tweetData:
    if "89" in tweet['id_str'] or "78" in tweet["id_str"]:
        tweetResults.append (tweet)        
endTime = time.time()
runTime_B = endTime - startTime
print(f"Query runtime: {runTime_B:4f} seconds")
print(tweetResults[0])


# c) Write and execute a SQL query to do the following. Time and report the runtime of your query. Find how many unique values are there in the “friends_count” column.

# In[6]:


startTime = time.time()

connection = sqlite3.connect('DSC450-Assignment9-Part1.db')
cursor = connection.cursor()
cursor.execute("SELECT COUNT(DISTINCT friends_count) as friends_countUnique FROM User;")
results_C = cursor.fetchone()[0]

endTime = time.time()
runTime_C = endTime - startTime
print(f"Query runtime: {runTime_C:4f} seconds")
print(f"Number of unique values in 'friends_count' column: {results_C}")


# d) Write the equivalent of the previous query in python (without using SQL) by reading it from the file. Time and report the runtime of your query. 

# In[7]:


startTime = time.time()
friendsCount = set()
for tweet in tweetData:
    user = tweet.get('user', {})
    friends_countNum = user.get('friends_count')
    friendsCount.add(friends_countNum)      
endTime = time.time()
runTime_D = endTime - startTime
print(f"Query runtime: {runTime_D:4f} seconds")
print(f"Number of unique values in 'friends_count' column: {len(friendsCount)}")


# e) Use python to plot the lengths of first 60 tweets (only 60, not all of the tweets) versus the length of the username for the user on a graph. Create a scatterplot. Submit both your python code and the resulting graph file.

# In[8]:


import matplotlib.pyplot as plt

tweetLengths = [len(tweet['text']) for tweet in tweetData[:60]]
usernameLengths = [len(tweet['user']['screen_name']) for tweet in tweetData[:60]]
plt.figure(figsize=(10, 6)) #Adjust bigger to see more disctinct points.
plt.scatter(usernameLengths, tweetLengths, color='darkred', alpha=0.5)
plt.title('Username vs. Tweet Lengths: First 60 Tweets')
plt.xlabel('Length of Username')
plt.ylabel('Length of Tweet')
plt.grid(True)
#plt.savefig('Part 1E Scatterplot.png')
plt.show()


# ### Part 2

# a) Create an index on userid in Tweet table in SQLite (submit SQL code for this question). These questions are as straightforward as they appear, you just need to create an index.

# In[9]:


connection = sqlite3.connect('DSC450-Assignment9-Part1.db')
cursor = connection.cursor()

cursor.execute("DROP INDEX IF EXISTS USER_IDX;")
cursor.execute("CREATE INDEX USER_IDX ON tweet(user_id);")
user_indexInfo = cursor.execute(f"PRAGMA index_info('USER_IDX');").fetchall()

if len(user_indexInfo) > 0:
    print(f"'USER_IDX' index exists.")
else: print(f"'USER_IDX' index does not exist.")

connection.commit()
connection.close()


# b) Create a composite index on (friends_count, screen_name) in User table (submit SQL code for this question).

# In[10]:


connection = sqlite3.connect('DSC450-Assignment9-Part1.db')
cursor = connection.cursor()

cursor.execute("DROP INDEX IF EXISTS COMP_IDX;")
cursor.execute("CREATE INDEX COMP_IDX ON User(friends_count, screen_name);")

comp_indexInfo = cursor.execute(f"PRAGMA index_info('COMP_IDX');").fetchall()

if len(comp_indexInfo) > 0:
    print(f"'COMP_IDX' index exists.")
else: print(f"'COMP_IDX' index does not exist.")
connection.commit()
connection.close()


# c) Create a materialized view (using CREATE TABLE AS because SQLite does not have full support for MVs) that answers the query in Part-1-a: Write and execute a SQL query to do the following: Find tweets where tweet id_str contains “89” or “78” anywhere in the column. Time and report the runtime of your query. Submit your SQL code.

# In[11]:


connection = sqlite3.connect('DSC450-Assignment9-Part1.db')
cursor = connection.cursor()

cursor.execute("DROP TABLE IF EXISTS PART_1A;")
cursor.execute("CREATE TABLE PART_1A AS SELECT * FROM tweet WHERE id_str LIKE '%89%' OR id_str LIKE '%78%';")
cursor.execute("SELECT * FROM PART_1A LIMIT 3;")
results = cursor.fetchall()
for row in results:
    print(row)

connection.commit()
connection.close()


# In[ ]:




