{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "bcdea68b-3569-4ffc-964d-ef43dd514509",
   "metadata": {},
   "outputs": [],
   "source": [
    "from datetime import timedelta\n",
    "from datetime import datetime\n",
    "from airflow import DAG\n",
    "from airflow.operators.python_operator import PythonOperator\n",
    "from airflow.utils.dates import days_ago\n",
    "from sqlalchemy_query_email import email_function\n",
    "\n",
    "# Default Arguments\n",
    "default_args = {\n",
    "    'owner': 'airflow',    \n",
    "    'start_date': airflow.utils.dates.days_ago(2),\n",
    "    # 'end_date': datetime(2018, 12, 30),\n",
    "    'depends_on_past': False,\n",
    "    'email': ['spotify.etl1@gmail.com'],\n",
    "    'email_on_failure': True,\n",
    "    'email_on_retry': True,\n",
    "    # If a task fails, retry it once after waiting\n",
    "    # at least 5 minutes\n",
    "    'retries': 1,\n",
    "    'retry_delay': timedelta(minutes=5),\n",
    "    }\n",
    "\n",
    "my_dag = DAG(\n",
    "    'spotify_email_dag',\n",
    "    default_args = default_args,\n",
    "    description= 'Spotify Weekly Email',\n",
    "    schedule_interval= '5 14 * * 0'\n",
    ")\n",
    "\n",
    "\n",
    "run_email = PythonOperator(\n",
    "    task_id='spotify_weekly_email',\n",
    "    python_callable= email_function,\n",
    "    dag=my_dag\n",
    ")\n",
    "run_email"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.10"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
