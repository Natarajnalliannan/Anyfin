
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


default_args = {
                    "owner": "airflow",
                    "depends_on_past": False,
                    "start_date": datetime(2023, 6, 1),
                    "email": ["airflow@airflow.com"],
                    "email_on_failure": False,
                    "email_on_retry": False,
                    "retries": 1,
                    "retry_delay": timedelta(minutes=1),
                }

dag = DAG("Anyfin_Application_time_line_data_set_pipeline", default_args=default_args, schedule_interval=timedelta(days=1), catchup=False)

def load_and_clean_data():
    """  applications_apth = string, cycles_path = string """

    applications_path = f'/usr/local/airflow/dags/applications.csv'
    cycles_path = f'/usr/local/airflow/dags/cycles.csv'

    # Correcting column_types based on the observed data in the applications DataFrame
    column_types = {
                        'id': 'str',
                        'created_at': 'str',
                        'status': 'str',
                        'customer_id': 'str',
                        'loan_id': 'str',
                        'email': 'str'
                    }

    df_application = pd.read_csv(applications_path, sep=';', dtype=column_types)
    df_cycles = pd.read_csv(cycles_path, sep=';')

    df_application = df_application[['id', 'created_at', 'status', 'customer_id', 'loan_id', 'email']]
    df_application = df_application.dropna(subset=['customer_id'])
    
    df_application.to_csv('/usr/local/airflow/dags/Task1_applications_output.csv', sep =";")
    df_cycles.to_csv('/usr/local/airflow/dags/Task1_cycles_output.csv', sep = ";" )
    ##End

def process_data():
    """df_applications = dataframe, df_cycles = dataframe"""
    
    applications_path = f'/usr/local/airflow/dags/Task1_applications_output.csv'
    cycles_path = f'/usr/local/airflow/dags/Task1_cycles_output.csv'
    
    df_applications = pd.read_csv(applications_path, sep=';')
    df_cycles = pd.read_csv(cycles_path, sep=';')

    df_applications['created_at'] = pd.to_datetime(df_applications['created_at']).dt.tz_localize(None)
    df_cycles['created_at'] = pd.to_datetime(df_cycles['created_at']).dt.tz_localize(None)

    df_applications = df_applications.sort_values(by='created_at')
    df_cycles = df_cycles.sort_values(by='created_at')

    df_applications['customer_id'] = df_applications['customer_id'].astype(str)
    df_cycles['customer_id'] = df_cycles['customer_id'].astype(str)
    df_cycles['paid'] = df_cycles['status'] == 'paid'
    df_cycles['unpaid'] = df_cycles['dpd'] > 0

    df_applications['num_applications_before_customer'] = df_applications.groupby('customer_id').cumcount()

    df_a2_loans = df_applications.copy()
    df_a2_loans =  df_a2_loans.drop_duplicates(subset=['loan_id'])
    df_a2_loans['number_of_loans_before_loan'] = df_a2_loans.groupby('customer_id')['loan_id'].cumcount()
    df_applications = df_applications.merge(df_a2_loans[['id', 'number_of_loans_before_loan']], on='id', how='left')

    def calculate_cycles_and_dpd(row) :
        """ row = each row from application data"""
        created_at = pd.to_datetime(row['created_at'])

        # Filter cycles before the application's created_at time
        cycles_before_a1 = df_cycles[(df_cycles['customer_id'] == row['customer_id']) &
                                    (df_cycles['created_at'] < created_at)]

        # Check if cycles_before_a1 is empty
        if cycles_before_a1.empty:
            print(f"No cycles found before {created_at} for customer {row['customer_id']}")
            # Initialize values to NaN if no cycles are found
            return pd.Series([
                0, 0, np.nan, np.nan, np.nan, np.nan
            ], index=[
                'paid_cycles_count', 'unpaid_cycles_count', 'avg_dpd',
                'max_dpd_30d', 'avg_dpd_60d', 'max_dpd_60d'
            ])

        # For cycles not more than 30 days before a1
        cycles_30d = cycles_before_a1[cycles_before_a1['created_at'] >= (created_at - pd.Timedelta(days=30))]
        avg_dpd = cycles_30d['dpd'].mean() if not cycles_30d.empty else np.nan
        max_dpd_30d = cycles_30d['dpd'].max() if not cycles_30d.empty else np.nan

        # For cycles not more than 60 days before a1
        cycles_60d = cycles_before_a1[cycles_before_a1['created_at'] >= (created_at - pd.Timedelta(days=60))]
        avg_dpd_60d = cycles_60d['dpd'].mean() if not cycles_60d.empty else np.nan
        max_dpd_60d = cycles_60d['dpd'].max() if not cycles_60d.empty else np.nan

        paid_cycles_count = cycles_before_a1['paid'].sum()
        unpaid_cycles_count = cycles_before_a1['unpaid'].sum()

        return pd.Series([
            paid_cycles_count, unpaid_cycles_count, avg_dpd,
            max_dpd_30d, avg_dpd_60d, max_dpd_60d
        ], index=[
            'paid_cycles_count', 'unpaid_cycles_count', 'avg_dpd',
            'max_dpd_30d', 'avg_dpd_60d', 'max_dpd_60d'
        ])

    df_applications[['paid_cycles_count', 'unpaid_cycles_count', 'avg_dpd',
                'max_dpd_30d', 'avg_dpd_60d', 'max_dpd_60d']] = df_applications.apply(calculate_cycles_and_dpd, axis=1)


    # Final DataFrame with selected and renamed columns
    final_df = df_applications[['id', 'created_at', 'customer_id',
                            'num_applications_before_customer', 'number_of_loans_before_loan',
                            'paid_cycles_count', 'unpaid_cycles_count', 'avg_dpd', 'max_dpd_30d',
                            'avg_dpd_60d', 'max_dpd_60d']].copy()
    final_df.columns = [
                            'application_id',
                            'application_created_at',
                            'customer_id',
                            'number_of_applications_from_customer_before',
                            'number_of_loans_from_customer_before',
                            'number_of_paid_cycles_for_customer_before',
                            'number_of_unpaid_cycles_for_customer_before',
                            'average_dpd_before_not_more_than_30_days_before',
                            'max_dpd_before_not_more_than_30_days_before',
                            'average_dpd_before_not_more_than_60_days_before',
                            'max_dpd_before_not_more_than_60_days_before'
                            
                        ]

    final_df.to_csv('/usr/local/airflow/dags/Task2_processed_output.csv',index=False, sep=';')
#End

def load_data_into_database():
    """df_final_result=dataframe"""
    
    print('Starting ... load data....')
    final_file_path = f'/usr/local/airflow/dags/Task2_processed_output.csv'  # Update this path

    df_final = pd.read_csv(final_file_path, sep=';')

    engine = create_engine('postgresql://username:supersecure@docker-airflow-postgres_db-1:5432/postgres')
    df_final.to_sql('Final_data_output', engine, if_exists="replace", index=False, )
#End

load_and_clean_data = PythonOperator(
    task_id='load_and_correct_data',
    python_callable=load_and_clean_data,
    dag=dag,
)

process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

load_data_into_database = PythonOperator(
    task_id='Load_data_into_database',
    python_callable=load_data_into_database,
    dag=dag,
)

load_and_clean_data >> process_data >> load_data_into_database

