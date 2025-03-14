import os
import requests
import pandas as pd

BASE_URL = "http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
KEY=os.getenv("MOVIE_KEY")

def gen_url(dt="20120101", url_param={}):
    "호출 URL 생성, url_param이 입력되면 multiMovieYn, repNationCd 처리"
    url = f"{BASE_URL}?key={KEY}&targetDt={dt}"
    # TODO = url_param 처리
    for k, v in url_param.items():
        url = url + f"&{k}={v}"   
        
    return url

# 강사님 코드
def call_api(dt="20120101", url_param={}):
    url = gen_url(dt, url_param)
    data = requests.get(url)
    j = data.json()
    return j['boxOfficeResult']['dailyBoxOfficeList']

# # 내가 쓴 코드
# def call_api():
#     response = requests.get(gen_url())
#     data = response.json()
#     return data['boxOfficeResult']['dailyBoxOfficeList']


def list2df(data: list, dt: str):
    df = pd.DataFrame(data)
    df['dt'] = dt
    return df

# def list2df(data, ymd):
#     import pandas as pd
#     df = pd.DataFrame(data)
#     df["dt"] = ymd
#     return df

def save_df(df, base_path):
    df.to_parquet(base_path, partition_cols=['dt'])
    save_path = f"{base_path}/dt={df['dt'][0]}"
    return save_path
    # ymd = "20210101"
    # data = call_api(dt=ymd)
    # df = list2df(data, ymd)
    # base_path = "~/temp/movie"
    
