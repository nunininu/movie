import os

BASE_URL = "http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
KEY=os.getenv("MOVIE_KEY")

def gen_url(dt="20120101", url_param={}):
    "호출 URL 생성, url_param이 입력되면 multiMovieYn, repNationCd 처리"
    url = f"{BASE_URL}?key={KEY}&targetDt={dt}"
    # TODO = url_param 처리
    for k, v in url_param.items():
        url = url + f"&{k}={v}"   
        
    return url

def call_api():
    return []