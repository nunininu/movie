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


def list2df(data: list, dt: str, url_params={}):
    df = pd.DataFrame(data)
    df['dt'] = dt
    # def test_list2df_check_num을 테스트하려면 call.py 에서 list2df를 바꿔야함
    for k, v in url_params.items():
        df[k] = v
        
    num_cols = ['rnum','rank','rankInten','salesAmt','audiCnt','audiAcc','scrnCnt','salesShare','salesInten','salesChange','audiInten','audiChange'] 
    # 1) 이렇게 쓰거나
    #for col_name in num_cols:     
        #df[col_name] = pd.to_numeric(df[col_name]) 
    # 2) 이렇게 써야함

    df[num_cols] = df[num_cols].apply(pd.to_numeric) # 내가 test_list2df_check_num에서 작성한 코드 -> 거기가 아니라 여기에 넣어었어야했음 
    return df

# def list2df(data, ymd):
#     import pandas as pd
#     df = pd.DataFrame(data)
#     df["dt"] = ymd
#     return df

#{"multiMovieYn":= "Y"]
def save_df(df, base_path, partitions=['dt']):
    df.to_parquet(base_path, partition_cols=partitions)
    save_path = base_path
    for p in partitions:
        save_path = save_path + f"/{p}={df[p][0]}"
    return save_path

#     ymd = "20210101"
#     data = call_api(dt=ymd)
#     df = list2df(data, ymd)
#     base_path = "~/temp/movie"
    
    
def fill_na_with_column(origin_df, c_name):
        df = origin_df.copy()
        for i, row in df.iterrows():
            if pd.isna(row[c_name]):
                same_movie_df = df[df["movieCd"] == row["movieCd"]]
                notna_idx = same_movie_df[c_name].dropna().first_valid_index()
                if notna_idx is not None:
                    df.at[i, c_name] = df.at[notna_idx, c_name]
        return df
    
def create_unique_ranked_df(df, drop_columns):
    df1 = fill_na_with_column(df, 'multiMovieYn')
    df2 = fill_na_with_column(df1, 'repNationCd')
    df3 = df2.drop(columns=drop_columns)
    unique_df = df3.drop_duplicates()
    return unique_df

# unique_df.loc[:, "rnum"] = unique_df["audiCnt"].rank(ascending=False).astype(int)
# unique_df.loc[:, "rank"] = unique_df["audiCnt"].rank(ascending=False).astype(int)

def re_ranking(unique_df):
    unique_df.loc[:, "rank"] = unique_df["audiCnt"].rank(ascending=False).astype(int)
    new_ranked_df = unique_df[['rank', 'movieCd', 'movieNm', 'audiCnt']]
    return new_ranked_df