import subprocess

from scrapy.cmdline import execute
from lxml.html import fromstring
from datetime import datetime
from typing import Iterable
from scrapy import Request
from doctor_trans import trans
import pandas as pd
import unicodedata
import asyncio
import random
import string
import scrapy
import time
import evpn
import os
import re


def df_cleaner(data_frame: pd.DataFrame) -> pd.DataFrame:
    print('Cleaning DataFrame...')
    data_frame = data_frame.astype(str)  # Convert all data to string
    data_frame.drop_duplicates(inplace=True)  # Remove duplicate data from DataFrame

    # Apply the function to all columns for Cleaning
    for column in data_frame.columns:
        data_frame[column] = data_frame[column].apply(set_na)  # Setting "N/A" where data is empty string
        data_frame[column] = data_frame[column].apply(remove_diacritics)  # Remove diacritics characters
        if 'title' in column:
            data_frame[column] = data_frame[column].str.replace('–', '')  # Remove specific punctuation 'dash' from name string
            data_frame[column] = data_frame[column].apply(remove_punctuation)  # Removing Punctuation from name text
        data_frame[column] = data_frame[column].apply(remove_extra_spaces)  # Remove extra spaces and newline characters from each column

    data_frame.replace(to_replace='nan', value=pd.NA, inplace=True)  # After cleaning, replace 'nan' strings back with actual NaN values
    data_frame.fillna(value='N/A', inplace=True)  # Replace NaN values with "N/A"
    print('DataFrame Cleaned...!')
    return data_frame


# Function to remove all punctuation
def remove_punctuation(text):
    return text if text == 'N/A' else ''.join(char for char in text if not unicodedata.category(char).startswith('P'))


def set_na(text: str) -> str:
    # Remove extra spaces (assuming remove_extra_spaces is a custom function)
    text = remove_extra_spaces(text=text)
    pattern = r'^([^\w\s]+)$'  # Define a regex pattern to match all the conditions in a single expression
    text = re.sub(pattern=pattern, repl='N/A', string=text)  # Replace matches with "N/A" using re.sub
    return text


# Function to remove Extra Spaces from Text
def remove_extra_spaces(text: str) -> str:
    return re.sub(pattern=r'\s+', repl=' ', string=text).strip()  # Regular expression to replace multiple spaces and newlines with a single space


def remove_diacritics(input_str):
    return ''.join(char for char in unicodedata.normalize('NFD', input_str) if not unicodedata.combining(char))


def extract_and_format_date(input_text):
    """
    Extracts a date in 'DD.MM.YYYY HH:mm' format from a string and converts it into 'YYYY-MM-DD' format.
    """
    # Regex to match 'DD.MM.YYYY HH:mm'
    # date_match = re.search(pattern=r'(\d{2})\.(\d{2})\.(\d{4}) \d{1,2}:\d{1,2}', string=input_text)
    date_match = re.search(pattern=r'(\d{2})[\.*\s](\d{2})[\.*\s](\d{4})[\.*\s]\d{1,2}:\d{1,2}', string=input_text)
    if date_match:
        day, month, year = date_match.groups()
        try:
            # Convert to datetime object and then to desired format
            date_obj = datetime.strptime(f"{day}.{month}.{year}", "%d.%m.%Y")
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            return 'N/A'  # Return "N/A" if date parsing fails
    return 'N/A'  # Return "N/A" if no date is found


def get_news_title(news_div) -> str:
    news_title = ' | '.join(news_div.xpath('./h1//text()')).strip()
    return news_title if news_title != '' else 'N/A'


def get_image_url(news_div) -> str:
    image_url_slug_list = news_div.xpath('./div[contains(@class, "full-text")]//img/@src')
    image_url = ' | '.join(['https://www.mvd.tj' + image_url_slug.strip() for image_url_slug in image_url_slug_list])
    return image_url if image_url else 'N/A'


def get_description(news_div) -> str:
    description = ' '.join(news_div.xpath('./div[contains(@class, "full-text")]/p//text()')).strip()
    return description if description != '' else 'N/A'


def get_news_date(news_div) -> str:
    news_date = ' '.join(news_div.xpath('.//div[contains(@class, "main-item-date") and contains(./span/@class, "fa-clock")]//text()'))
    news_date = extract_and_format_date(news_date)
    return news_date


class MvdTjTajikistanSpider(scrapy.Spider):
    name = "mvd_tj_tajikistan"

    def __init__(self):
        self.start = time.time()
        super().__init__()
        print('Connecting to VPN (PAKISTAN)')
        self.api = evpn.ExpressVpnApi()  # Connecting to VPN (PAKISTAN)
        self.api.connect(country_id='198')  # PAKISTAN country code for vpn
        time.sleep(10)  # keep some time delay before starting scraping because connecting
        print('VPN Connected!' if self.api.is_connected else 'VPN Not Connected!')

        self.final_data_list = list()  # List of data to make DataFrame then Excel

        # Path to store the Excel file can be customized by the user
        self.excel_path = r"../Excel_Files"  # Client can customize their Excel file path here (default: govtsites > govtsites > Excel_Files)
        os.makedirs(self.excel_path, exist_ok=True)  # Create Folder if not exists
        self.filename_native = fr"{self.excel_path}/{self.name}_native.xlsx"  # Native Filename with Scrape Date
        self.filename_translated = fr"{self.excel_path}/{self.name}_translated.xlsx"  # English Filename with Scrape Date
        self.browsers = ["chrome110", "edge99", "safari15_5"]

        self.cookies = {
            '_ym_uid': '1732862092889177211',
            '_ym_d': '1732862092',
            'dle_skin': 'tj',
            '_ym_isad': '2',
            'PHPSESSID': '1e3176e2e6fb697a38264a89b162ca8d',
        }
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Referer': 'https://www.mvd.tj/tj/ruydodho/',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }

    def start_requests(self) -> Iterable[Request]:
        # Sending request on an api which gives news detail page's url in html text in response json.
        url = 'https://www.mvd.tj/tj/ruydodho/page/1/'
        yield scrapy.Request(url=url, method="GET", cookies=self.cookies, headers=self.headers, dont_filter=True, callback=self.parse,
                             cb_kwargs={'url': url}, meta={'impersonate': random.choice(self.browsers)})

    def parse(self, response, **kwargs):
        url = kwargs.get('url', 'N/A')
        parsed_tree = fromstring(response.text)
        news_detail_page_urls = parsed_tree.xpath('//div[@class="news-item clearfix"]/a[@class="side-item-link"]/@href')
        for news_page_url in news_detail_page_urls:
            print('Sending request on news page url:', news_page_url)
            # Send request on criminal detail  page url
            yield scrapy.Request(url=news_page_url, headers=self.headers, cookies=self.cookies, method='GET', callback=self.detail_parse, dont_filter=True,
                                 meta={'impersonate': random.choice(self.browsers)}, cb_kwargs={'url': url, 'news_page_url': news_page_url})

        # Handle Pagination here
        # if Next-Page-Button -> Pagination-Request else Stop-Pagination
        next_page_url = ' '.join(parsed_tree.xpath('//span[@class="pnext"]/a/@href'))
        if next_page_url:
            print('Sending request on:', next_page_url)
            yield scrapy.Request(url=next_page_url, method="GET", cookies=self.cookies, headers=self.headers, dont_filter=True, callback=self.parse,
                                 cb_kwargs={'url': next_page_url}, meta={'impersonate': random.choice(self.browsers)})
        else:
            print('Pagination not found after:', url)

    def detail_parse(self, response, **kwargs):
        parsed_tree = fromstring(response.text)
        news_div = parsed_tree.xpath('//article[contains(@class, "full ignore-select")]')[0]
        data_dict = dict()
        data_dict['url'] = kwargs.get('url')
        data_dict['news_page_url'] = kwargs.get('news_page_url')
        data_dict['news_title'] = get_news_title(news_div)
        data_dict['image_url'] = get_image_url(news_div)
        data_dict['description'] = get_description(news_div)
        data_dict['news_date'] = get_news_date(news_div)

        print(data_dict)
        self.final_data_list.append(data_dict)

    def close(self, reason):
        print('closing spider...')
        print("Converting List of Dictionaries into DataFrame, then into Excel file...")
        try:
            print("Creating Native sheet...")
            native_data_df = pd.DataFrame(self.final_data_list)
            native_data_df = df_cleaner(data_frame=native_data_df)  # Apply the function to all columns for Cleaning

            # Translate the DataFrame to English and return translated DataFrame
            # tranlated_df = trans(data_df, input_lang='tg-TJ', output_lang='en')

            with pd.ExcelWriter(path=self.filename_native, engine='xlsxwriter', engine_kwargs={"options": {'strings_to_urls': False}}) as writer:
                native_data_df.insert(loc=0, column='id', value=range(1, len(native_data_df) + 1))  # Add 'id' column at position 1
                native_data_df.to_excel(excel_writer=writer, index=False)
            print("Native Excel file Successfully created.")

            # Run the translation script with filenames passed as arguments
            try:
                subprocess.run(
                    args=["python", "translate_and_save.py", self.filename_native, self.filename_translated],  # Define the filenames as arguments
                    check=True
                )
                print("Translation completed successfully.")
            except subprocess.CalledProcessError as e:
                print(f"Error during translation: {e}")

        except Exception as e:
            print('Error while Generating Excel file:', e)

        if self.api.is_connected:  # Disconnecting VPN if it's still connected
            self.api.disconnect()
            print('VPN Connected!' if self.api.is_connected else 'VPN Disconnected!')

        end = time.time()
        print(f'Scraping done in {end - self.start} seconds.')


if __name__ == '__main__':
    execute(f'scrapy crawl {MvdTjTajikistanSpider.name}'.split())
