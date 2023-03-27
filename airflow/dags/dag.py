from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from operators.care_providers import (
    clean_care_providers,
    create_care_providers_datacube,
)
from operators.general import cleanup, download_file, edit_enum
from operators.population import clean_population, create_population_datacube

with DAG(
    dag_id="data-cubes",
    start_date=datetime(2023, 3, 27),
    schedule=None,
    tags=["NDBI046"],
    catchup=False,
    description="DAG producing population and care providers data cubes",
) as dag:
    d_enum = PythonOperator(
        task_id="download_county_enum",
        python_callable=download_file,
        op_args=(
            [
                "https://skoda.projekty.ms.mff.cuni.cz/ndbi046/seminars/02/%C4%8D%C3%ADseln%C3%ADk-okres%C5%AF-vazba-101-nad%C5%99%C3%ADzen%C3%BD.csv",
                "region_enum.csv",
            ]
        ),
    )
    d_enum.doc = "Downloads enum mapping LAU county codes to NUTS."

    d_pop = PythonOperator(
        task_id="download_population",
        python_callable=download_file,
        op_args=(
            [
                "https://www.czso.cz/documents/10180/184344914/130141-22data2021.csv",
                "population2021.csv",
            ]
        ),
    )
    d_pop.doc = "Downloads population 2021 dataset."

    d_cp = PythonOperator(
        task_id="download_providers",
        python_callable=download_file,
        op_args=(
            [
                "https://opendata.mzcr.cz/data/nrpzs/narodni-registr-poskytovatelu-zdravotnich-sluzeb.csv",
                "care_providers.csv",
            ]
        ),
    )
    d_cp.doc = "Downloads care providers dataset."

    clean_cp = PythonOperator(
        task_id="clean_providers",
        python_callable=clean_care_providers,
        op_args=(["./tmp/care_providers.csv"]),
    )
    clean_cp.doc = (
        "Cleans care providers dataset. Keeps only needed columns and complete values."
    )

    clean_pop = PythonOperator(
        task_id="clean_population",
        python_callable=clean_population,
        op_args=(["./tmp/population2021.csv"]),
    )
    clean_pop.doc = (
        "Cleans population dataset. Keeps only needed columns and complete values."
    )

    edit_enum = PythonOperator(
        task_id="edit_enum",
        python_callable=edit_enum,
        op_args=(["./tmp/region_enum.csv", "./tmp/care_providers.csv"]),
    )
    edit_enum.doc = (
        "Cleans enum, adds region and country names from care providers dataset."
    )

    create_pop = PythonOperator(
        task_id="create_population_cube",
        python_callable=create_population_datacube,
        op_args=(["./tmp/population2021.csv", "./tmp/region_enum.csv"]),
    )
    create_pop.doc = "Creates population datacube."

    create_cp = PythonOperator(
        task_id="create_providers_cube",
        python_callable=create_care_providers_datacube,
        op_args=(["./tmp/care_providers.csv"]),
    )
    create_cp.doc = "Creates care providers datacube."

    clean = PythonOperator(
        task_id="cleanup", python_callable=cleanup, trigger_rule=TriggerRule.ALL_DONE
    )
    clean.doc = "Cleanup of temporary files."

    d_pop >> clean_pop
    d_cp >> clean_cp >> create_cp
    [d_enum, clean_cp] >> edit_enum
    [clean_pop, edit_enum] >> create_pop
    [create_cp, create_pop] >> clean
