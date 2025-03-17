from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import (
        BranchPythonOperator, 
        PythonVirtualenvOperator,
)

# # Directed Acyclic Graph
with DAG(
    "merge_data"
    # schedule="@hourly",
    # start_date=pendulum.datetime(2025, 3, 12, tz="Asia/Seoul"),
) as dag:
    
    start = EmptyOperator(task_id="start")
        
    branch_op = BranchPythonOperator(
        task_id="branch.op", 
    )
    
    rm_dir = BashOperator(
        task_id="rm.dir", 
        bash_command=""
    )
    
    echo_task = BashOperator(
        task_id="echo.task", 
        bash_command=""
    )
    
    get_start = EmptyOperator(task_id="get.start")
    
    
    no_param = PythonVirtualenvOperator(
        task_id="no.param"
    )
    
    multi_n = PythonVirtualenvOperator(
        task_id="multi.n"
    )
    
    multi_y = PythonVirtualenvOperator(
        task_id="multi.y"
    )
    
    nation_f = PythonVirtualenvOperator(
        task_id="nation.f"
    )
    
    nation_k = PythonVirtualenvOperator(
        task_id="nation.k"
    )
    
    
    get_end = EmptyOperator(task_id="get.end")
    
    merge_data = BranchPythonOperator(
        task_id="merge.data"
    )
    
    end = EmptyOperator(task_id="end")
    

    
start >> branch_op >> [rm_dir, echo_task] >> get_start
get_start >> [no_param, multi_n, multi_y, nation_f, nation_k]
[no_param, multi_n, multi_y, nation_f, nation_k] >> get_end >> merge_data >> end



# get.end >> merge.data >> end
# echo.task
# [rm.dir, echo.task]
# rm.dir >> get.start >> no.param >> 
# >> no.param
# >> multi.n
# >> multi.y
# >> nation.f
# >> nation.k

