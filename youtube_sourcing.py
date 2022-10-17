from typing import List, Tuple

from selenium import webdriver as wd
from bs4 import BeautifulSoup
import time
import pandas as pd
import s3fs
from constants import S3_BUCKET_NAME, AWS_KEY_ID, AWS_SECRET_KEY

ops = wd.ChromeOptions()

ops.add_extension("/Users/yelee/Desktop/CS4480/dislikes_extension.zip")

driver = wd.Chrome(executable_path="/Users/yelee/Desktop/CS4480/chromedriver", chrome_options=ops)
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
    channel_id = element.find("meta", itemprop="channelId")["content"]
    channel_name = element.find("link", itemprop="name")["content"]
    video_id = element.find("meta", itemprop="videoId")["content"]
    video_title = element.find("meta", itemprop="name")["content"]
    description = element.find("meta", itemprop="description")["content"]
    genre = element.find("meta", itemprop="genre")["content"]
    views = element.find("meta", itemprop="interactionCount")["content"]
    likes_dislikes = element.find_all('yt-formatted-string',
                                      {'id': 'text', 'class': 'style-scope ytd-toggle-button-renderer style-text'})

    likes = likes_dislikes[0].string
    dislikes = likes_dislikes[1].string

    published_date = element.find("meta", itemprop="datePublished")["content"]

    return {"channel_id": channel_id, "channel_name": channel_name, "video_id": video_id,
            "video_title": video_title, "video_description": description, "genre": genre, "views": views,
            "likes": likes, "dislikes": dislikes, "published_date": published_date}


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


def upload_file_using_client(df, file_name) -> None:
    bytes_to_write = df.to_csv(None).encode()
    fs = s3fs.S3FileSystem(key=AWS_KEY_ID, secret=AWS_SECRET_KEY)
    bucket_name = S3_BUCKET_NAME

    with fs.open(f"s3://{bucket_name}/{file_name}.csv", 'wb') as f:
        f.write(bytes_to_write)


html_source = get_html_source(driver=driver, url=url)
close_pop_up()

soup = BeautifulSoup(html_source, 'html.parser')

video_meta_data = get_video_meta_data(soup)

comment_tuple = get_comments(soup)

comment_dict = {"channel_id": video_meta_data["channel_id"], "video_id": video_meta_data["video_id"],
                "user_id": comment_tuple[0], "comments": comment_tuple[1]}

upload_file_using_client(pd.DataFrame([video_meta_data]),
                         f"{video_meta_data['channel_id']}/{video_meta_data['video_id']}/videos")
upload_file_using_client(pd.DataFrame([comment_dict]),
                         f"{video_meta_data['channel_id']}/{video_meta_data['video_id']}/comments")
