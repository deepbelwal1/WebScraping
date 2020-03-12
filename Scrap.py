from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
from datetime import date
from tqdm import tqdm


class Scrap:
    def get_and_parse_url(self, url):
        result = requests.get(url)
        soup = BeautifulSoup(result.text, 'html.parser')
        stk_name = soup.find("h1", attrs={'class': 'pcstname'})
        stock_name = stk_name.text
        all_data = soup.find("div", attrs={'class': 'nsert'})
        m = all_data.text.split("\n")
        comm_sentiment = soup.find("div", attrs={'class': 'commounity_senti'})
        community_sentiment = comm_sentiment.text
        tech_rating = soup.find("div", attrs={'class': 'mt15 CTR pb20'})
        technical_rating = tech_rating.find('a')['title']
        try:
            buy_percentage = re.search("buy_percentage = parseInt\('(.*?)'\);", community_sentiment).groups()[0]
            sell_percentage = re.search("sell_percentage = parseInt\('(.*?)'\);", community_sentiment).groups()[0]
            hold_percentage = re.search("hold_percentage = parseInt\('(.*?)'\);", community_sentiment).groups()[0]
        except:
            buy_percentage = "NA"
            sell_percentage = "NA"
            hold_percentage = "NA"
        community_sentiment_data = [buy_percentage, sell_percentage, hold_percentage]
        while "" in m:
            m.remove("")
        data_of_one_url = self.make_dataframe(technical_rating, m, stock_name, community_sentiment_data)
        return data_of_one_url

    def make_dataframe(self, technical_rating, data, stock_name, community_sentiment_data):
        # Calling DataFrame constructor
        df = pd.DataFrame()
        try:
            df = df.append({"Stock_Name": stock_name,
                            "Price_Change": data[3],
                            "Prev_close": data[14],
                            "Open_Price": data[16],
                            "52_Week_Low": data[20],
                            "52_Week_High": data[22],
                            "Technical_Rating": technical_rating,
                            "buy_percentage": community_sentiment_data[0],
                            "sell_percentage": community_sentiment_data[1],
                            "hold_percentage": community_sentiment_data[2]
                            }, ignore_index=True)
        except:
            print ("error found")
        return df

    def get_dynamic_url(self):
        url = "https://www.moneycontrol.com/india/stockpricequote/"
        list_of_url = []
        result = requests.get(url)
        soup = BeautifulSoup(result.text, 'html.parser')
        all_url = soup.find("table", attrs={"class": "pcq_tbl MT10"})
        for a in all_url.find_all('a', href=True):
            list_of_url.append(a['href'])
        self.get_category_for_list_of_url(list_of_url[:-1])

    def get_category_for_list_of_url(self, list_of_url):
        # TODO: Change all variable names, make it function specific
        categorization_dict = {}
        for i in tqdm(list_of_url):
            category = i.split('/')[-3:]
            if categorization_dict.get(category[0]):
                categorization_dict[category[0]].append(i)
            else:
                categorization_dict[category[0]] = []
                categorization_dict[category[0]].append(i)
        self.get_data_for_each_url(categorization_dict)

    def get_data_for_each_url(self, categorization_dict):
        dfObj = pd.DataFrame(
            columns=['category', 'URL', 'stock_name', 'price_change', 'prev_close', 'open_price', 'fifty_two_week_low',
                     'fifty_two_week_high', 'Technical_Rating', "buy_percentage", "sell_percentage", "hold_percentage",
                     "Date"])
        for key, value in tqdm(categorization_dict.items()):
            url_list = value
            for url in tqdm(url_list):
                data = self.get_and_parse_url(url)
                try:
                    dfObj = dfObj.append(
                        {"category": key, "URL": url, "stock_name": data["Stock_Name"].values[0],
                         "price_change": data["Price_Change"].values[0],
                         "prev_close": data["Prev_close"].values[0], "open_price": data["Open_Price"].values[0],
                         "fifty_two_week_low": data["52_Week_Low"].values[0],
                         "fifty_two_week_high": data["52_Week_High"].values[0],
                         "Technical_Rating": data["Technical_Rating"].values[0],
                         "buy_percentage": data["buy_percentage"].values[0],
                         "sell_percentage": data["sell_percentage"].values[0],
                         "hold_percentage": data["hold_percentage"].values[0],
                         "Date": str(date.today())},
                        ignore_index=True)
                except Exception as e:
                    print(dfObj, e)
        dfObj.to_csv("money_control_data.csv")


scap_obj = Scrap()
scap_obj.get_dynamic_url()
