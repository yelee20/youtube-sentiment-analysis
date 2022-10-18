from typing import List, Tuple
from selenium.webdriver.common.keys import Keys
from selenium import webdriver as wd
from bs4 import BeautifulSoup
import time
import pandas as pd
import s3fs
from constants import S3_BUCKET_NAME, AWS_KEY_ID, AWS_SECRET_KEY

options = wd.ChromeOptions()

options.add_extension("/Users/yelee/Desktop/CS4480/dislikes_extension.zip")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--disable-web-security")

driver = wd.Chrome(executable_path="/Users/yelee/Desktop/CS4480/chromedriver", chrome_options=options)
url = 'https://www.youtube.com/watch?v=ZqyAI1L_Seo'
url_list = ["https://www.youtube.com/watch?v=35BkHichD2M", "https://www.youtube.com/watch?v=ZqyAI1L_Seo"]

def get_html_source(driver, url: str):
    driver.implicitly_wait(3)
    driver.get(url)

    time.sleep(3)
    # 스크롤 내리기
    last_page_height = driver.execute_script("return document.documentElement.scrollHeight")

    while True:
        driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
        time.sleep(4.0)
        new_page_height = driver.execute_script("return document.documentElement.scrollHeight")
        if new_page_height == last_page_height:
            time.sleep(4.0)
            if new_page_height == driver.execute_script("return document.documentElement.scrollHeight"):
                break
        else:
            last_page_height = new_page_height

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

comment_dict = {"user_id": comment_tuple[0], "comments": comment_tuple[1]}

# upload video info to s3
video_df = pd.DataFrame(video_meta_data, index=[0])

upload_file_using_client(video_df,
                         f"{video_meta_data['channel_id']}/{video_meta_data['video_id']}/videos")

# upload comments to s3
comment_df = pd.DataFrame(comment_dict, index=[_ for _ in range(len(comment_tuple[0]))])
comment_df["channel_id"] = video_meta_data["channel_id"]
comment_df["video_id"] = video_meta_data["video_id"]

upload_file_using_client(comment_df,
                         f"{video_meta_data['channel_id']}/{video_meta_data['video_id']}/comments")
