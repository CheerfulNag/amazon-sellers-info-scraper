
def table_pages_generator(country,link):
    dict1 = {
        "US":"usa",
        "GB":"uk",
        "DE":"germany",
        "FR":"france",
        "IT":"italy",
        "ES":"spain",
        "JP":"japan",
        "IN":"india"
    }
    for x in range(1,26):
        pages_links.append(link.format(dict1[country],x))
    
def list_scraper(url):
    r = requests.get(url,headers=headers)
    soup = BeautifulSoup(r.text,'html.parser')
    table = soup.select_one('table#report')
    table = table.select_one('tbody')
    rows = table.find_all('tr')

    for row in rows:
        rank = row.select_one('td.rank').text
        rank = rank.replace(" ",'').split('\n')[0]
        name = row.find('a').text
        detailed_link = f"https://www.sellerratings.com{row.find('a')['href']}"
        reviews = row.select("td.number")
        last_30d_reviews = reviews[0].text
        all_reviews = reviews[1].text
        record_part = (rank,name,last_30d_reviews,all_reviews,detailed_link)
        records_part.append(record_part)

def detailed_link_scraper(x):
    rank = (x[0].replace(",",""))
    name = x[1]
    last_30d_reviews = x[2]
    all_reviews = x[3]
    detailed_link = x[4]
    country_dict = {
        "com":"US",
        "co.uk":"GB",
        "de":"DE",
        "fr":"FR",
        "it":"IT",
        "es":"ES",
        "co.jp":"JP",
        "in":"IN"
    }

    r = requests.get(detailed_link,headers=headers)
    soup = BeautifulSoup(r.text,"html.parser")
    stars = soup.select('div.rating-stats span')
    stars = f"{stars[1].text}/{stars[2].text}"
    amazon_link = soup.select_one('p:-soup-contains("For most recent reviews checkout ") a')['href']
    seller_id = amazon_link.split('=')[1]
    domain = (amazon_link.split('www.amazon.'))[1].split('/')[0]
    country = country_dict[domain]
    record = (rank,name,detailed_link,last_30d_reviews,all_reviews,amazon_link,country,seller_id) 
    records.append(record)

def sort_function(x):
    return int(x[0])

def sort_function_parrent():
    records.sort(key=sort_function)

def csv_file_creator():
    with open(f"results.csv","w",newline="",encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["rank","seller_name","seller_internal_url","30_days_reviews","lifetime_reviews","seller_amazon_url","marketplace_country_code","amazon_seller_id"])    

def csv_saver_adder():
    with open(f"results.csv","a",newline="",encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows([x for x in records])

def one_country_main(country_name):

    global records
    records = []

    global pages_links
    global records_part
    pages_links = []
    records_part = []
    global headers
    headers = {
        "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.186 Safari/537.36",
    }
    link = 'https://www.sellerratings.com/amazon/{}?page={}'
    table_pages_generator(country_name,link)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(list_scraper,pages_links)
    print(f"Scraping detailed links for {country_name}")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(detailed_link_scraper,records_part)
    
    sort_function_parrent()
    csv_saver_adder()
    print(f"{country_name.upper()} done.")

def folder_creator():
    try:  
        os.mkdir(os.path.join(os.getcwd(),"results_folder"))
        print(f"Results folder has been created.")
    except:
        pass
        print(f"Results folder already exist.")
    os.chdir(os.path.join(os.getcwd(),'results_folder'))


def main(countries):
    csv_file_creator()
    print("Scraping")
    if countries == "all":
        one_country_main("US")
        time.sleep(100)
        one_country_main("GB")
        time.sleep(100)
        one_country_main("DE")
        time.sleep(100)
        one_country_main("FR")
        time.sleep(100)
        one_country_main("IT")
        time.sleep(100)
        one_country_main("ES")
        time.sleep(100)
        one_country_main("JP")
        time.sleep(100)
        one_country_main("IN")
    else:
        for x in countries.split(","):
            one_country_main(x)
    print("Complete")
    

import requests
from bs4 import BeautifulSoup
import csv
import concurrent.futures
import os
import time


print(
    """
    #############INSTURCTIONS#################
    #For country selection enter names you need format "DE,GB,JP"
    #If you want to scrap data for all countries - "all"
    #Countries:
    # US, GB, DE, FR, IT, ES, JP, IN
    """
)

w = input("Enter: ")

main(w)

