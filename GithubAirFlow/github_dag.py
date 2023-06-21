from datetime import timedelta,datetime

from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

import os

default_args = {
	"owner":"Aurelius",
	"depends_on_past":False,
	"start_date": datetime(2023,6,17),
	"email": ["s.kocyigit4224@gmail.com"],
	"email_on_failure":False,
	"email_on_retry":False,
	"retries":1,
	"retry_delay":timedelta(minutes=1)
}


def checkifRepoExitsts():
	file = os.path.exists("/home/ubuntu/gitTry/AirflowFirstExample")
	if(file):
		return "removeandfetch"
	else:
		return "fetch"


def run():
	import sys
	sys.path.insert(1,"/home/ubuntu/gitTry/AirflowFirstExample/")
	from hello import helloWorld
	helloWorld()


dag = DAG(
	"github_airflow",
	default_args=default_args,
	description ="Github Airflow with AWS"
	)

Start = EmptyOperator(
	task_id = "Start",
	dag = dag)


FetchOrNot = BranchPythonOperator(task_id = "FetchOrNot",
	python_callable = checkifRepoExitsts,
	dag = dag
	)



RemoveAndFetch = BashOperator(
	task_id = "removeandfetch",
	bash_command = "/home/ubuntu/scripts/removeandfetch.sh ",
	trigger_rule = TriggerRule.ALL_SUCCESS,
	dag = dag)

Fetch = BashOperator(
	task_id = "fetch",
	bash_command = "/home/ubuntu/scripts/fetch.sh ",
	trigger_rule = TriggerRule.ALL_SUCCESS,
	dag = dag)

End = PythonOperator(task_id = "End", python_callable=run,trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, dag=dag)


Start >> FetchOrNot
FetchOrNot >> Label("Fetch") >> Fetch
FetchOrNot >> Label("Remove exiting and fetch") >> RemoveAndFetch
Fetch >> End
RemoveAndFetch >> End





