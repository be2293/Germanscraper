import GermanScraperFunctions as scraper
import pandas as pd
url = "https://portal.dnb.de/opac/showShortList?currentPosition=0&currentResultId=jhr+within+%221500+*%22+and+jhr+within+%22*+1930%22%26any%26books%26online%26sg6"
df = scraper.AltScrapeBerlin(url)
df.to_csv("AltScrapeBerlin.csv")
scraper.AltLoadData(df)
