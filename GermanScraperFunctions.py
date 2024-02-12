import requests
from bs4 import BeautifulSoup
import pandas as pd 
import math
import re
import mysql.connector
from mysql.connector import (connection)
import pymysql.cursors
import pymysql

#Select correct url of search
url = "https://portal.dnb.de/opac/showShortList?currentPosition=0&currentResultId=jhr+within+%221500+*%22+and+jhr+within+%22*+1930%22%26any%26books%26online%26sg6"


#Scrapes the number of pages from the first page of the results 
def NumberOfPages(url):
    
    #obtain the html text of the url (Input your own user agent)
    headers = {"User-Agent": "Your_User_Agent_Here"}
    response = requests.get(url, headers= headers)
    soup = BeautifulSoup(response.text, "html.parser")
    
    #Find the line of html code with the number of results in the search 
    NumberPagesRaw = soup.find('span', attrs = {'class': 'amount'}).text

    #clean out the number of results from the line and divide to get the number of pages
    IndexofVon = NumberPagesRaw.find("von ")
    NumberPages = math.ceil(int(NumberPagesRaw[IndexofVon + len("von "):])/10)

    #return number of pages
    return NumberPages

#For any given url, finds the next page in the results search. If none exists, then returns NA
def FindNextPage(url):
    
    #get the html text of the url (input your user_agent)
    headers = {"User-Agent": "Your User_Agent Here"}
    response = requests.get(url, headers= headers)
    soup = BeautifulSoup(response.text, "html.parser")
    
    #attempts to find a line with the correct tag and construct the url of the next page. If it fails, it returns NA
    try:
        NextPageLink = "https://portal.dnb.de/" + soup.find('a', attrs={'title': 'zur nächsten Trefferseite blättern'})['href']
        return NextPageLink
    except TypeError: 
        return "NA"

#Takes in a url and returns all the lines referring to the 10 entries in the url
def GetResults(url):

    #get the html of the url
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    #Fills a list of all the lines referring to the 10 entries in the url (uncleaned)
    results = []
    for result in soup.find_all('a', id=lambda x: x and 'recordLink' in x):
        results.append(str(result))
    return results

#Strips the title of the book from a line found by the GetResults() function
def StripTitle(entry):

    #removes all extraneous tabs and new lines
    CleanEntry = entry.replace('\n', '').replace('\t', '')
    
    #Find the indices of characters indicating the beginning and end of the title
    IndexofBr = CleanEntry.find("<br/>")
    EndofHref = CleanEntry.find(">")+1
    StrippedTitle = CleanEntry[EndofHref:IndexofBr]

    #returns the title
    return StrippedTitle

#Strips the year of the book from a line found by the GetResults() function
def StripYear(entry):
    
    #strips out the extraneous parts of the entry 
    FindEnd = entry.find(">")
    CleanEntry = str(entry[FindEnd:])

    #collects a list of all numbers in the rest of the entry, and then selects the first one in the correct date range, or returns NA
    RawYears = re.findall(r'\d+', CleanEntry)
    for x in range(len(RawYears)):
        if 1499 < int(RawYears[x])<1931:
            return RawYears[x]
    return "NA"
    
       
#Scrapes the german library
def ScrapeBerlin(url):
    #checks whether there are pages to scrape. If it does, continues
    if NumberOfPages(url) > 0:
        ResultList = []
        IteratedUrl = url 

        #Starting with the first page, adds the results to the list, and then makes the url of the next page the new url
        for x in range(NumberOfPages(url)-1):
            if IteratedUrl != "NA":
                ResultList.extend(GetResults(IteratedUrl))
                IteratedUrl=FindNextPage(IteratedUrl)
        
        #scrapes out the year and title of each entry
        YearList = []
        TitleList = []
        for result in ResultList:
            YearList.append(StripYear(result))
            TitleList.append(StripTitle(result))

        #creates a dataframe out of the results thus obtained and returns it
        ResultDataFrame = pd.DataFrame({'Title':TitleList, 'Year':YearList})
        return ResultDataFrame 

#Loads the data collected into an SQL database    
def LoadData(data):
    
    #sets up the database connection
    connection = pymysql.connect(host = 'Your_Host_Here',
                             user = 'Your_User_Here',
                             password = "Your_Password_Here",
                             db = 'Your_Database_Here',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            
            #creates a table in SQL if it doesn't exist
            cursor.execute(
            """CREATE TABLE IF NOT EXISTS germanbooks
                (title VARCHAR(255), year VARCHAR(255), PRIMARY KEY (title))"""
            )
            
            #turns the dataset into a list of tuples
            DataTuples = list(data.itertuples(index=False, name=None))
            
            #loads each tuple into the table
            for DataTuple in DataTuples:
                    cursor.execute(
                    "INSERT IGNORE INTO germanbooks (title, year) VALUES(%s, %s)", tuple(DataTuple))
            
            #saves changes
            connection.commit()

#This alternate scraping method is the one used for this data. It is built to remove magazines/periodicals from the German library, since we are interested
#primarily in books. The method we used is to check if the numerics-removed book has more than 3 duplicates (indicating a periodical), and remove if so.
def AltScrapeBerlin(url):
    #checks whether there are pages to scrape. If it does, continues
    if NumberOfPages(url) > 0:
        ResultList = []
        IteratedUrl = url 

        #Starting with the first page, adds the results to the list, and then makes the url of the next page the new url
        for x in range(NumberOfPages(url)-1):
            if IteratedUrl != "NA":
                ResultList.extend(GetResults(IteratedUrl))
                IteratedUrl=FindNextPage(IteratedUrl)
        
        #scrapes out the year and title of each entry
                #In addition, scrapes out an alt-title which is the numerics-removed version of the title
        YearList = []
        TitleList = []
        altTitleList = []
        for result in ResultList:
            YearList.append(StripYear(result))
            TitleList.append(StripTitle(result))
            altTitleList.append(re.sub(r'[0-9]+', '', StripTitle(result)))

        #creates a dataframe out of the results thus obtained and returns it
        ResultDataFrame = pd.DataFrame({'Title':TitleList, 'Year':YearList, 'AltTitleList':altTitleList})
        
        #counts the number of duplicate alttitles, removes any with more than 3, and then drops the count value
        ResultDataFrame['count_column'] = ResultDataFrame.groupby('AltTitleList')['AltTitleList'].transform('count')
        ResultDataFrame = ResultDataFrame[ResultDataFrame['count_column']<4]
        ResultDataFrame = ResultDataFrame.drop('count_column', axis = 1)
        return ResultDataFrame 
#Alternate database loading corresponding to the alternate scraping technique.
def AltLoadData(data):
    
    #sets up the database connection
    connection = pymysql.connect(host = 'Your_Host_Here',
                             user = 'Your_User_Here',
                             password = "Your_Password_Here",
                             db = 'Your_Database_Here',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            
            #creates a table in SQL if it doesn't exist
            cursor.execute(
            """CREATE TABLE IF NOT EXISTS germanbooksv2
                (title VARCHAR(255), year VARCHAR(255), alttitle VARCHAR(255), PRIMARY KEY (title))"""
            )
            
            #turns the dataset into a list of tuples
            DataTuples = list(data.itertuples(index=False, name=None))
            
            #loads each tuple into the table
            for DataTuple in DataTuples:
                    cursor.execute(
                    "INSERT IGNORE INTO germanbooksv2 (title, year, alttitle) VALUES(%s, %s, %s)", tuple(DataTuple))
            
            #saves changes
            connection.commit()
