from typing import List, Tuple

from selenium import webdriver as wd
from bs4 import BeautifulSoup
import time
import pandas as pd
import s3fs

from constants import S3_BUCKET_NAME, AWS_KEY_ID, AWS_SECRET_KEY

driver = wd.Chrome(executable_path="/Users/yelee/Desktop/CS4480/chromedriver")
url = 'https://www.youtube.com/watch?v=35BkHichD2M'


def get_html_source(driver, url: str):
    driver.implicitly_wait(3)

    driver.get(url)

    time.sleep(1.5)

    driver.execute_script("window.scrollTo(0, 800)")
    time.sleep(3)
    last_height = driver.execute_script("return document.documentElement.scrollHeight")

    while True:
        driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
        time.sleep(1.5)

        new_height = driver.execute_script("return document.documentElement.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    time.sleep(1.5)

    return driver.page_source


def close_pop_up():
    try:
        driver.find_element_by_css_selector("#dismiss-button > a").click()
    except:
        pass


def get_video_meta_data(element):
    channel = element.find("link", itemprop="name")["content"]
    title = element.find("meta", itemprop="name")["content"]
    # description = element.find("meta", itemprop="description")["content"]
    views = element.find("meta", itemprop="interactionCount")["content"]
    published_date = element.find("meta", itemprop="datePublished")["content"]
    return [channel, title, views, published_date]


def get_comments(element) -> Tuple[List, List]:
    id_list = element.select("div#header-author > h3 > #author-text > span")
    comment_list = element.select("yt-formatted-string#content-text")

    id_final = []
    comment_final = []
    for i in range(len(comment_list)):
        temp_id = id_list[i].text
        temp_id = temp_id.replace('\n', '')
        temp_id = temp_id.replace('\t', '')
        temp_id = temp_id.replace('    ', '')
        id_final.append(temp_id)

        temp_comment = comment_list[i].text
        temp_comment = temp_comment.replace('\n', '')
        temp_comment = temp_comment.replace('\t', '')
        temp_comment = temp_comment.replace('    ', '')
        comment_final.append(temp_comment)
    return id_final, comment_final


def upload_file_using_client(df) -> None:
    bytes_to_write = df.to_csv(None).encode()
    fs = s3fs.S3FileSystem(key=AWS_KEY_ID, secret=AWS_SECRET_KEY)
    bucket_name = S3_BUCKET_NAME

    with fs.open(f"s3://{bucket_name}/comments.csv", 'wb') as f:
        f.write(bytes_to_write)


html_source = get_html_source(driver=driver, url=url)
close_pop_up()

soup = BeautifulSoup(html_source, 'html.parser')

meta_data = get_video_meta_data(soup)

comments_tuple = get_comments(soup)

pd_data = {"channel_name": meta_data[0], "video_title": meta_data[1], "views": meta_data[2],
           "published_date": meta_data[3],
           "user_id": comments_tuple[0], "comments": comments_tuple[1]}

youtube_pd = pd.DataFrame(pd_data)

upload_file_using_client(youtube_pd)
