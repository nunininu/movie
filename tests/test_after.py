import pandas as pd
import os
from movie.api.after import combine_df, fillna_meta, read_df_or_none, save_with_mkdir

# 경국님 코드

def test_fillna_meta():
    previous_df = pd.DataFrame(
        {
            "movieCd": ["1001", "1002", "1003"],
            "multiMovieYn": ["Y", "Y", "N"],
            "repNationCd": ["K", "F", None],
        }
    )


    current_df = pd.DataFrame(
        {
            "movieCd": ["1001", "1003", "1004"],
            "multiMovieYn": [None, "Y", "Y"],
            "repNationCd": [None, "F", "K"],
        }
    )

    r_df = fillna_meta(previous_df, current_df)

    assert not r_df.isnull().values.any(), "결과 데이터프레임에 NaN 또는 None 값이 있습니다!"

    
def test_fillna_meta_none_previous_df():
    previous_df = None

    current_df = pd.DataFrame(
        {
            "movieCd": ["1001", "1003", "1004"],
            "multiMovieYn": [None, "Y", "Y"],
            "repNationCd": [None, "F", "K"],
        }
    )

    r_df = fillna_meta(previous_df, current_df)

    assert r_df.equals(current_df), "r_df는 current_df와 동일해야 합니다!"
    
def test_read_parquet_or_none():
    """parquet 파일이 없을 때 None 리턴 여부 테스트"""
    df = read_df_or_none("~/temp/none/none.parquet")  # 해당 파일은 없음
    assert df is None, "파일이 없을 때 None을 리턴해야 합니다!"


def test_save_with_mkdir():
    test_df = pd.DataFrame(
        {
            "movieCd": ["1001", "1002", "1003"],
            "multiMovieYn": ["Y", "N", "Y"],
            "repNationCd": ["K", "F", "K"],
        }
    )

    # 테스트할 파일 경로 (임시 경로)

    test_parquet_path = "./test_meta/meta_test.parquet"

    # 테스트 함수 호출
    saved_path = save_with_mkdir(test_df, test_parquet_path)
    # 저장된 경로가 실제로 존재하는지 확인
    assert os.path.exists(saved_path), f"파일이 저장되지 않았습니다: {saved_path}"
    
    loaded_df = pd.read_parquet(saved_path)
    assert test_df.equals(loaded_df), "저장된 데이터가 원본 DataFrame과 다릅니다!"
    
    # 테스트 후 파일 삭제
    os.remove(saved_path)
    
def test_combine_df():
    meta_df = pd.read_parquet("~/data/movie_after/meta/meta.parquet")
    movie_df = pd.read_parquet("~/data/movies/dailyboxoffice/dt=20240101")
    
    assert len(movie_df) == 50
    r = combine_df(meta_df=meta_df, current_df=movie_df, ds_nodash="20240101")
    assert len(r) == 50