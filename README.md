# German National Library Scraper

*Created by Benjamin Eyal, February 2024.*

This program scrapes the catalog of the German National Library for technical books published between 1500 and 1930.

The program records the following data for each book:
- Title
- Publication year
- Title with Numerics Removed (AltTitle)

## Search details
The searches made by this scraper are equivalent to filling out the following fields in the einfache (normal) search (https://portal.dnb.de/opac/showSearchForm#top) form of the catalog with expert mode turned on:
- [x] Years Published/Created: From 1500 to 1930 
- [x] Type of Material: Book, Electronic Resources
- [x] Subject Group: 6 (tecnology, DDC)
- [x] Language: German

The scraper will scrape all of the results pages. If a search does not have any results, the program will not have any output. If it does, it will output a CSV and a SQL database of the results 

## Saving Results
The program will save a csv to your working directory, and an SQL table to your active database. 


## Functions
If you want to scrape a topic, or group of topics, load the functions in GermanScraperfunctions.R to your workspace. Then, run the following code of GermanScraperMain:
```py
import GermanScraperFunctions as scraper
import pandas as pd
url = "https://portal.dnb.de/opac/showShortList?currentPosition=0&currentResultId=jhr+within+%221500+*%22+and+jhr+within+%22*+1930%22%26any%26books%26online%26sg6"
df = scraper.AltScrapeBerlin(url)
df.to_csv("AltScrapeBerlin.csv")
scraper.AltLoadData(df)
```
The function AltScrapeBerlin is structured as follows:
1. Check whether the search has results. If not, immediately abort. 
2. Extract the number of results pages associated with that search.
3. Loop through each page number and:
   1. Extract all of the book info from the page.
   2. Save the results of the page to a dataframe
   3. Extract all numerics from the title, and create an alttitle special column
   4. Remove any duplicate alttitles past 3 entries
4. The program ends after scraping the last page.
The function AltLoadData is structured as follows:
1. Load the data to a SQL table associate with your connection

## note on alteration:
Becuase the German library does not separate periodicals from books in their online resources, we have decided to use a method for removing them. If alttitle shows more than 3 entries associated with a numerics-removed title, all entries are removed, presuming they are a repeating periodical or journal. 