from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator


@dag
def orchestrate():

    @task
    def ingest_cdc():
        return "CDC data ingested"
    
    @task.bash
    def clean_target():
        return """
        set -e

        cd /opt/airflow/walmart_project

        rm -rf target
        rm -rf logs
        """

    @task.bash
    def source_freshness():
        return """
        set -e

        cd /opt/airflow/walmart_project

        rm -rf target

        dbt source freshness \
          --project-dir /opt/airflow/walmart_project \
          --profiles-dir /opt/airflow/walmart_project
        """
    silver_technical = BashOperator(
        task_id="silver_technical",
        bash_command='cd /opt/airflow/walmart_project && dbt run --select silver_t',    
    )
    
    silver_technical_test = BashOperator(
        task_id="silver_technical_test",
        bash_command='cd /opt/airflow/walmart_project && dbt test --select silver_t',    
    )
    
    silver_business = BashOperator(
        task_id="silver_business",
        bash_command='cd /opt/airflow/walmart_project && dbt run --select silver_b',    
    )
    
    silver_business_test = BashOperator(
        task_id="silver_business_test",
        bash_command='cd /opt/airflow/walmart_project && dbt test --select silver_b',    
        
    )
    
    gold_ephemeral = BashOperator(
        task_id="gold_ephemeral",
        bash_command='cd /opt/airflow/walmart_project && dbt run --select gold/ephemeral',    
    )
    
    gold_dimensions = BashOperator(
        task_id="gold_dimensions",
        bash_command='cd /opt/airflow/walmart_project && dbt snapshot',    
        
    )
    
    gold_facts = BashOperator(
        task_id="gold_facts",
        bash_command='cd /opt/airflow/walmart_project && dbt run --select gold/fact',    
    )

    ingest_cdc()>> clean_target() >> source_freshness() >> silver_technical >> silver_technical_test >> silver_business >> silver_business_test >> gold_ephemeral >> gold_dimensions >> gold_facts


orchestrate_dag = orchestrate()