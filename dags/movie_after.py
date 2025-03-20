from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        BranchPythonOperator, 
        PythonVirtualenvOperator,
)

from airflow.sensors.filesystem import FileSensor
from pprint import pprint

DAG_ID = "movie_after"

with DAG(
    DAG_ID,
    default_args={
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=5,
    description='movie',
    schedule="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 5),
    catchup=True,
    tags=['api', 'movie', 'sensor'],
) as dag:
    REQUIREMENTS = ["git+https://github.com/nunininu/movie.git@250320.3"]
    BASE_DIR = f"~/data/{DAG_ID}"
       
    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end')
    
    check_done = FileSensor(
        task_id="check_done",
        filepath="/home/sgcho/data/movies/done/dailyboxoffice/{{ ds_nodash }}/_DONE",
        fs_conn_id="fs_after_movie",
        poke_interval=180,  # 3분마다 체크
        timeout=3600,  # 1시간 후 타임아웃
        mode="reschedule",  # 리소스를 점유하지 않고 절약하는 방식
    )
    
    def fn_gen_meta(ds_nodash, base_path, **kwargs):
        import json
        import pandas as pd
        #print(json.dumps(kwargs, indent=4, ensure_ascii=False))
        # json.dumps(kwargs, indent=4, ensure_ascii=False)
        df = pd.read_parquet(f'/home/sgcho/data/movies/merge/dailyboxoffice/dt={ds_nodash}')[['movieCd', 'multiMovieYn', 'repNationCd']]
        
        # IF 없으면 ... 위 df 를 그냥 meta 로 저장
        import os
        meta_df = pd.read_parquet(f"{base_path}/meta/meta.parquet")
        
        # df, meta_df 를 fill
        
        # 저장 meta.parquet
        
        # df2 = pd.DataFrame()
        # df = df1.combine_first(df2) ???????????????????????????
        # df.to_parquet(f"{base_path}/meta/meta.parquet")
        # TODO f"{base_path}/meta/meta.parquet -> 경로로 저장


    gen_meta = PythonVirtualenvOperator(
        task_id="gen.meta",
        python_callable=fn_gen_meta,
        requirements=REQUIREMENTS,
        system_site_packages=False,
    )

    
    # def fn_merge_data(ds_nodash, BASE_DIR):
    #     from movie.api.call import fill_na_with_column, gen_unique_df, re_ranking, save_df
    #     import pandas as pd
    #     #print(ds_nodash)
    #     # df read => ~/data/movies/dailyboxoffice/dt=20240101
    #     
    #     path = f"{BASE_DIR}/dt={ds_nodash}"
    #     df = pd.read_parquet(path)
    #     df1 = fill_na_with_column(df, 'multiMovieYn')
    #     df2 = fill_na_with_column(df1, 'repNationCd')
    #     drop_columns=['rnum', 'rank', 'rankInten', 'salesShare']
    #     unique_df = gen_unique_df(df=df2, drop_columns=drop_columns)
    #     new_ranked_df = re_ranking(unique_df, dt=ds_nodash)
    #     merge_save_path = save_df(new_ranked_df, f"/home/sgcho/data/movies/merge/dailyboxoffice")
    #     print(merge_save_path + "<================ 에 저장됨")   
    
    
    def fn_gen_movie(base_path, ds_nodash, **kwargs):
        import json
        from movie.api.call import fillna_meta
        import pandas as pd
        print(json.dumps(kwargs, indent=4, ensure_ascii=False))
        print(f"base_path: {base_path}")
        # df = pd.read_parquet(f"{base_path}/meta/meta.parquet")
        
        meta_df = pd.read_parquet(f"{base_path}/meta/meta.parquet")
        df = pd.read_parquet(f'/home/sgcho/data/movies/merge/dailyboxoffice/dt={ds_nodash}')
        
        # meta_df, df 를 join 해서 최대한 댜양성, 해외 컬럼을 채워서 저장
        
        
        partitions=['dt', 'multiMovieYn', 'repNationCd']
        df.to_parquet(f"{base_path}/dailyboxoffice", partition_cols=partitions)
        
        
        
        # import json
        # print(json.dumps(kwargs, indent=4, ensure_ascii=False))
        # print(f"base_path: {base_path}")
        # TODO -> f"{base_path}/dailyboxoffice/ 생성
        # movie airflow 의 merge.data 와 같은 동작 ( meta.parquet 사용 )
        # 파티션은 dt, multiMovieYn, repNationCd

    
    gen_movie = PythonVirtualenvOperator(
        task_id="gen.movie",
        python_callable=fn_gen_movie,
        requirements=REQUIREMENTS,
        system_site_packages=False,
        op_kwargs={"base_path": BASE_DIR},
    )

    make_done = BashOperator(
        task_id="make.done",
        bash_command="""
        DONE_BASE=$BASE_DIR/done
        echo $DONE_BASE
        mkdir -p $DONE_BASE/{{ ds_nodash }}
        touch $DONE_BASE/{{ ds_nodash }}/_DONE
        """,
        env={'BASE_DIR':BASE_DIR},
        append_env = True
    )

    start >> check_done >> gen_meta >> gen_movie >> make_done >> end
