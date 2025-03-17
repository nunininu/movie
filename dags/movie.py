from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        BranchPythonOperator, 
        PythonVirtualenvOperator,
)

with DAG(
    'movie',
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
    end_date=datetime(2024, 12, 31),
    catchup=True,
    tags=['api', 'movie'],
) as dag:
    REQUIREMENTS = ["git+https://github.com/nunininu/movie.git@0.1.0"]
    BASE_DIR = "~/data/movies/dailyboxoffice"

    def branch_fun(ds_nodash):
        import os
        if os.path.exists(f"{BASE_DIR}/dt={ds_nodash}"):
            return rm_dir.task_id #task_id로 리턴하는 방법
            #return "rm.dir" #task실제이름으로 리턴하는 방법
        else:
            return "get.start", "echo.task" #task실제이름으로 리턴하는 방법

    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun
    )
    
    def fn_merge_data(ds_nodash):
        print(ds_nodash)
        
    
    merge_data = PythonVirtualenvOperator(
        task_id='merge.data',
        python_callable=fn_merge_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
    )

    def common_get_data(ds_nodash, url_param):
        print(ds_nodash, url_param)
        # TODO
        # API로 불러온 데이터를 
        # BASE_DIR/dt=20240101/repNationCd=K/****.parquet
        # STEP 1 - GIT에서 PIP를 설치하고
        # BASE_DIR/dt=202040101/ 먼저 해서 잘 되면
        # repNationCd=k/도 붙여준다
    
    multi_y = PythonVirtualenvOperator(
        task_id='multi.y',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS
        op_kwargs="url_param": {"multiMovieYn": "Y"}
        )

    multi_n = PythonVirtualenvOperator(
        task_id='multi.n',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs="url_param": {"multiMovieYn": "N"}
        )
    

    nation_k = PythonVirtualenvOperator(
        task_id='nation.k',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs="url_param": {"repNationCd": "K"}
        )
    

    nation_f = PythonVirtualenvOperator(
        task_id='nation.f',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs="url_param": {"repNationCd": "F"}
        )
    
    no_param = PythonVirtualenvOperator(
        task_id='no.param',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs="url_param": {}
        )

    rm_dir = BashOperator(task_id='rm.dir',
                          bash_command='rm -rf $BASE_DIR/dt={{ ds_nodash }}',
                          env={'BASE_DIR': BASE_DIR})

    echo_task = BashOperator(
        task_id='echo.task',
        bash_command="echo 'task'"
    )
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    get_start = EmptyOperator(task_id='get.start',
                              trigger_rule="all_done")
    get_end = EmptyOperator(task_id='get.end')
    

    start >> branch_op

    branch_op >> rm_dir >> get_start
    branch_op >> get_start
    branch_op >> echo_task
    get_start >> [multi_y, multi_n, nation_k, nation_f, no_param] >> get_end

    get_end >> merge_data >> end

## 내가 썼던 코드 
# from airflow.operators.bash import BashOperator
# from airflow.operators.empty import EmptyOperator
# from airflow.models.dag import DAG
# from airflow.operators.python import (
#         BranchPythonOperator, 
#         PythonVirtualenvOperator,
# )

# # # Directed Acyclic Graph
# with DAG(
#     "merge_data"
#     # schedule="@hourly",
#     # start_date=pendulum.datetime(2025, 3, 12, tz="Asia/Seoul"),
# ) as dag:
    
#     start = EmptyOperator(task_id="start")
        
#     def dummy_fun():
#         pass
    
#     branch_op = BranchPythonOperator(
#         task_id="branch.op",
#         python_callable=dummy_fun
#     )
    
#     rm_dir = BashOperator(
#         task_id="rm.dir", 
#         bash_command=""
#     )
    
#     echo_task = BashOperator(
#         task_id="echo.task", 
#         bash_command=""
#     )
    
#     get_start = EmptyOperator(task_id="get.start")
    
    
#     no_param = PythonVirtualenvOperator(
#         task_id="no.param",
#         python_callable=dummy_fun
#     )
    
#     multi_n = PythonVirtualenvOperator(
#         task_id="multi.n",
#         python_callable=dummy_fun
#     )
    
#     multi_y = PythonVirtualenvOperator(
#         task_id="multi.y",
#         python_callable=dummy_fun
#     )
    
#     nation_f = PythonVirtualenvOperator(
#         task_id="nation.f",
#         python_callable=dummy_fun
#     )
    
#     nation_k = PythonVirtualenvOperator(
#         task_id="nation.k",
#         python_callable=dummy_fun
#     )
    
#     get_end = EmptyOperator(task_id="get.end")
    
#     merge_data = BranchPythonOperator(
#         task_id="merge.data",
#         python_callable=dummy_fun
#     )
    
#     end = EmptyOperator(task_id="end")
    

    
# start >> branch_op >> [rm_dir, echo_task] >> get_start
# get_start >> [no_param, multi_n, multi_y, nation_f, nation_k]
# [no_param, multi_n, multi_y, nation_f, nation_k] >> get_end >> merge_data >> end



# get.end >> merge.data >> end
# echo.task
# [rm.dir, echo.task]
# rm.dir >> get.start >> no.param >> 
# >> no.param
# >> multi.n
# >> multi.y
# >> nation.f
# >> nation.k

