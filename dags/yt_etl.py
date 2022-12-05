import sqlalchemy
import pandas as pd
import sqlite3
import json
import re
import requests
import config
from google.cloud import language_v1

def classify_text(text_content):
    client = language_v1.LanguageServiceClient()
    type_ = language_v1.Document.Type.PLAIN_TEXT
    language = "en"
    document = {"content": text_content, "type_": type_, "language": language}
    content_categories_version = (language_v1.ClassificationModelOptions.V2Model.ContentCategoriesVersion.V2)
    response = client.classify_text(request = {
        "document": document,
        "classification_model_options": {
            "v2_model": {
                "content_categories_version": content_categories_version
            }
        }
    })
    if len(response.categories) > 0:
        return response.categories[0].name
    else:
        return ""

def run_yt_etl():
    db_file = "C:/Users/glliao/AppData/Local/Google/Chrome/User Data/Default/History"
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    sql_query = """
    SELECT
    urls.url,
    datetime(visit_time/1000000+strftime('%s','1601-01-01'),'unixepoch','localtime') AS vt
    FROM urls JOIN visits
    ON vt>datetime('now','-7 day') AND urls.id=visits.url
    """
    cursor.execute(sql_query)
    history_data = cursor.fetchall()
    conn.close()
    id_time_list = []
    for url, visit_time in history_data:
        match = re.search("\?v=", url)
        if match:
            start_index = match.end()
            video_id = url[start_index:start_index + 11]
            id_time_list.append((video_id, visit_time))
    api_key = config.API_KEY
    time_data = []
    mood_data = []
    title_data = []
    channel_data = []
    category_data = []
    for id, time in id_time_list:
        res = requests.get("https://www.googleapis.com/youtube/v3/videos?part=snippet&id={}&key={}".format(id, api_key))
        if res.status_code == 200:
            info = json.loads(res.text)
            if len(info["items"]) > 0:
                video = info["items"][0]["snippet"]
                title = video["title"]
                channel = video["channelTitle"]
                category = classify_text(title)
                matches = []
                mood_level = 4
                matches.append(re.search("Entertainment|News", category))
                matches.append(re.search("Food|Online", category))
                matches.append(re.search("Sports|Travel", category))
                matches.append(re.search("Reference|Science", category))
                matches.append(re.search("Hobbies|Health", category))
                matches.append(re.search("Computers|Internet", category))
                if matches[0]:
                    mood_level = 1
                elif matches[1]:
                    mood_level = 2
                elif matches[2]:
                    mood_level = 3
                elif matches[3]:
                    mood_level = 5
                elif matches[4]:
                    mood_level = 6
                elif matches[5]:
                    mood_level = 7
                time_data.append(time)
                mood_data.append(mood_level)
                title_data.append(title)
                channel_data.append(channel)
                category_data.append(category)
    DATABASE_LOCATION = "sqlite:///yt_data.sqlite"
    yt_dict = {
        "timestamp": time_data,
        "mood_level": mood_data,
        "title": title_data,
        "channel": channel_data,
        "category": category_data
    }
    yt_df = pd.DataFrame(yt_dict, columns = [*yt_dict])
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect("yt_data.sqlite")
    cursor = conn.cursor()
    sql_query = """
    CREATE TABLE IF NOT EXISTS yt_data(
        timestamp VARCHAR(200),
        mood_level INT(10),
        title VARCHAR(200),
        channel VARCHAR(200),
        category VARCHAR(200)
    )
    """
    cursor.execute(sql_query)
    try:
        yt_df.to_sql("yt_data", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")
    conn.close()
if __name__ == "__main__":
    run_yt_etl()