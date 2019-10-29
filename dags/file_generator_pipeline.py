from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from custom_plugin.operators.run_id_generator import RunIdGenerator

print(RunIdGenerator)

default_args = {
    'start_date': datetime(2019, 10, 5),
    'email': ['airflow_notification@thisisadummydomain.com'],
    'email_on_failure': False
}

dag = DAG('file_generator_pipeline',
          description='Hello world DAG',
          default_args=default_args,
          schedule_interval='0 17 * * *'
          )

with dag:
    print('ts: {{ ts_nodash }}')
    print('ts: {{ run_id }}')

    # run_id = RunIdGenerator.custom_run_id('{{ ts }}')
    print_job_id = BashOperator(
        task_id='unique_id',
        bash_command="echo run: {{ ts_nodash }}",
    )

    generate_first_file = BashOperator(
        task_id='generate_first_file',
        bash_command='python /var/storage/file-generator/generate_file.py --run_id {{ ts_nodash }} --line "first file line" --filename file1.txt',
    )

    generate_second_file = BashOperator(
        task_id='generate_second_file',
        bash_command='python /var/storage/file-generator/generate_file.py --run_id {{ ts_nodash }} --line "second file line" --filename file2.txt',
    )

    merge_file = BashOperator(
        task_id='merge',
        bash_command='python /var/storage/file-generator/merge.py --run_id {{ ts_nodash }}',
    )

    print_job_id >> [generate_first_file, generate_second_file] >> merge_file
