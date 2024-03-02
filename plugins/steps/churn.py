import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import inspect, Table, MetaData, Column, Integer, Float, String, DateTime, UniqueConstraint
import pandas as pd


def extract(**kwargs):
	# ваш код здесь #
    hook = PostgresHook('source_db')
    conn = hook.get_conn()
    sql = f"""
    select
        c.customer_id, c.begin_date, c.end_date, c.type, c.paperless_billing, c.payment_method, c.monthly_charges, c.total_charges,
        i.internet_service, i.online_security, i.online_backup, i.device_protection, i.tech_support, i.streaming_tv, i.streaming_movies,
        p.gender, p.senior_citizen, p.partner, p.dependents,
        ph.multiple_lines
    from contracts as c
    left join internet as i on i.customer_id = c.customer_id
    left join personal as p on p.customer_id = c.customer_id
    left join phone as ph on ph.customer_id = c.customer_id
    """
    data = pd.read_sql(sql, conn)
    conn.close()
    ti = kwargs['ti']
    ti.xcom_push(key='extracted_data', value=data)

    
def transform(**kwargs):
    """
    #### Transform task
    """
    ti = kwargs['ti'] # получение объекта task_instance
    data = ti.xcom_pull(task_ids='extract', key='extracted_data') # выгрузка данных из task_instance
    data['target'] = (data['end_date'] != 'No').astype(int) # логика функции
    data['end_date'].replace({'No': None}, inplace=True)
    ti.xcom_push('transformed_data', data) # вместо return отправляем данные передатчику task_instance 

def load(**kwargs):
    # ваш код здесь #
    hook = PostgresHook('destination_db')
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform',key = 'transformed_data')
    hook.insert_rows(
        table="alt_users_churn",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['customer_id'],
        rows=data.values.tolist()
    )

    
def create_table():
    from sqlalchemy import inspect, Table, MetaData, Column, Integer, Float, String, DateTime, UniqueConstraint
    hook = PostgresHook('destination_db')
    engine = hook.get_sqlalchemy_engine()
    metadata = MetaData()
    users_churn = Table('alt_users_churn', metadata,
                        Column('id', Integer, primary_key=True, autoincrement=True),
                        Column('customer_id', String, unique=True),
                        Column('begin_date', DateTime),
                        Column('end_date', DateTime),
                        Column('type', String),
                        Column('paperless_billing', String),
                        Column('payment_method', String),
                        Column('monthly_charges', Float),
                        Column('total_charges', Float),
                        Column('internet_service', String),
                        Column('online_security', String),
                        Column('online_backup', String),
                        Column('device_protection', String),
                        Column('tech_support', String),
                        Column('streaming_tv', String),
                        Column('streaming_movies', String),
                        Column('gender', String),
                        Column('senior_citizen', Integer),
                        Column('partner', String),
                        Column('dependents', String),
                        Column('multiple_lines', String),
                        Column('target', Integer),
                        UniqueConstraint('customer_id', name='alt_unique_customer_id_constraint')
                        )
    metadata.create_all(engine)
        
        

