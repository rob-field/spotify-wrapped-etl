{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "af0a293c-e9a0-44c7-ac39-c1e39da4de6f",
   "metadata": {},
   "outputs": [],
   "source": [
    "from datetime import timedelta\n",
    "\n",
    "import airflow\n",
    "from airflow import DAG\n",
    "from airflow.operators.bash import BashOperator\n",
    "from airflow.operators.python_operator import PythonOperator\n",
    "\n",
    "from spotify_etl_script import email_function\n",
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
    "# Instantiate DAG\n",
    "dag = DAG(\n",
    "    'etl',\n",
    "    default_args=default_args,\n",
    "    description='Spotify ETL DAG',\n",
    "    # Run DAG every 12 hours\n",
    "    schedule_interval=\"0 */12 * * *\",\n",
    ")\n",
    "\n",
    "\n",
    "run_etl = PythonOperator(\n",
    "    task_id='spotify_etl',\n",
    "    python_callable= spotify_etl,\n",
    "    dag=my_dag\n",
    ")\n",
    "run_etl"
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
