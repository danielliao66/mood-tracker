import sqlalchemy
import pandas as pd
import sqlite3
import json
import re
import datetime
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

def main():
    with open("history.json", "r", encoding="utf-8") as file:
        history_data = json.load(file)
    id_time_list = []
    for site in history_data:
        match = re.search("\?v=", site["url"])
        if match:
            start_index = match.end()
            video_id = site["url"][start_index:start_index + 11]
            timestamp = datetime.datetime(1970, 1, 1) + datetime.timedelta(microseconds=site["visitTime"]*1e3-1.8e10)
            id_time_list.append((video_id, timestamp.strftime("%m/%d/%Y, %H:%M:%S")))
    api_key = config.API_KEY
    time_data = []
    mood_data = []
    for id_time_pair in id_time_list:
        res = requests.get("https://www.googleapis.com/youtube/v3/videos?part=snippet&id={}&key={}".format(id_time_pair[0], api_key))
        if res.status_code == 200:
            info = json.loads(res.text)
            if len(info["items"]) > 0:
                category = classify_text(info["items"][0]["snippet"]["title"])
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
                time_data.append(id_time_pair[1])
                mood_data.append(mood_level)
    DATABASE_LOCATION = "sqlite:///time_mood_data.sqlite"
    time_mood_dict = {
        "timestamp": time_data,
        "mood_level": mood_data
    }
    time_mood_df = pd.DataFrame(time_mood_dict, columns = ["timestamp", "mood_level"])
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect("time_mood_data.sqlite")
    cursor = conn.cursor()
    sql_query = """
    CREATE TABLE IF NOT EXISTS time_mood_data(
        timestamp VARCHAR(200),
        mood_level INT(10)
    )
    """
    cursor.execute(sql_query)
    try:
        time_mood_df.to_sql("time_mood_data", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")
    conn.close()
    
if __name__ == "__main__":
    main()