import pandas as pd
import os


# 내가 쓴 코드
# def fillna_meta(previous_df, current_df):
#     if previous_df is not None:
#         previous_df.set_index("movieCd", inplace=True)
#         current_df.set_index("movieCd", inplace=True)
    
#         merged_df = previous_df.merge(current_df, on="movieCd", how="outer", suffixes=("_A", "_B"))

#         for i in range(0,len(merged_df)):    
#             if pd.isna(merged_df.iloc[i, 0]):
#                 merged_df.iloc[i, 0] = merged_df.iloc[i, 2]
#             if pd.isna(merged_df.iloc[i, 1]):
#                 merged_df.iloc[i, 1] = merged_df.iloc[i, 3]

#         multiMovieYn = merged_df.copy()['multiMovieYn_A'].tolist()     
#         repNationCd = merged_df.copy()['repNationCd_A'].tolist()
#         data = {"multiMovieYn": multiMovieYn, "repNationCd": repNationCd}
#         df = pd.DataFrame(data)
#         df["movieCd"] = merged_df.index

#         # movieCd 열을 맨 왼쪽으로 이동 
#         df.insert(0, 'movieCd', df.pop('movieCd'))
#         return df
    
#     else:
#         return current_df

# 경국님 코드=======================================================================================

def fillna_meta(previous_df: pd.DataFrame, current_df: pd.DataFrame) -> pd.DataFrame:
    if previous_df is None:
        return current_df[["movieCd", "multiMovieYn", "repNationCd"]]

    # movieCd 기준으로 outer join
    merged_df = previous_df.merge(
        current_df, on="movieCd", how="outer", suffixes=("_A", "_B")
    )
    print(merged_df)

    # NaN 값 대체 (A 값 우선, 없으면 B 값 사용)
    merged_df["multiMovieYn"] = merged_df["multiMovieYn_A"].combine_first(
        merged_df["multiMovieYn_B"]
    )
    merged_df["repNationCd"] = merged_df["repNationCd_A"].combine_first(
        merged_df["repNationCd_B"]
    )

    # 불필요한 컬럼 제거
    meta_df = merged_df[["movieCd", "multiMovieYn", "repNationCd"]]

    return meta_df


    
def combine_df(meta_df: pd.DataFrame, current_df: pd.DataFrame, ds_nodash: str) -> pd.DataFrame:
    merged_df = current_df.merge(meta_df, on="movieCd", how="left", suffixes=("_current", "_meta"))
    merged_df["multiMovieYn"] = merged_df["multiMovieYn_meta"].combine_first(merged_df["multiMovieYn_current"])
    merged_df["repNationCd"] = merged_df["repNationCd_meta"].combine_first(merged_df["repNationCd_current"])
    final_df = merged_df[current_df.columns]
    final_df['dt'] = ds_nodash
    return final_df


def read_df_or_none(parquet_path: str) -> pd.DataFrame:
    if os.path.exists(parquet_path):
        df = pd.read_parquet(parquet_path)
    else:
        df = None
        
    return df



def save_with_mkdir(df: pd.DataFrame, parquet_path: str) -> str:
    """디렉토리가 없으면 생성하고, DataFrame을 parquet 파일로 저장한 후 경로 반환"""
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
    
    # DataFrame을 parquet 형식으로 저장
    df.to_parquet(parquet_path)
    
    # 저장된 파일의 절대 경로 반환
    absolute_path = os.path.abspath(parquet_path)
    print(f"파일이 성공적으로 저장되었습니다: {absolute_path}")
    
    return absolute_path