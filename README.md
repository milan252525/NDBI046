# NDBI046 Introduction to Data Engineering
https://skoda.projekty.ms.mff.cuni.cz/ndbi046/

# Task 1
## System requirements
Python 3.10+

## Installation instructions
1. Clone this repository
2. Optionally, create a virtual environment (`python -m virtualenv venv`, `source venv/bin/activate`)
3. Install required libraries `pip install -r requirements.txt`
4. To generate data cubes run respective script in `cubes` directory
    - `python cubes/care_providers.py` (output in `out/care_providers.ttl`)
    - `python cubes/population.py` (output in `out/population.ttl`)
5. Check integrity constraints using `python queries.py`

## Information
### Care providers data cube
- Script located in `cubes/care_providers.py`
  - Import `get_cube` function to use the cube elsewhere 
  - If ran as a main file the cube will be generated in RDF Turtle file (`out/care_providers.ttl`)
- Uses [Národní registr poskytovatelů zdravotních služeb](https://data.gov.cz/datov%C3%A1-sada?iri=https://data.gov.cz/zdroj/datov%C3%A9-sady/https---opendata.mzcr.cz-api-3-action-package_show-id-nrpzs) dataset
- dimensions:
  - county
  - region
  - field of care
- measures:
  - number of care providers

### Population 2021 data cube
- Script located in `cubes/population.py`
  - Import `get_cube` function to use the cube elsewhere 
  - If ran as a main file the cube will be generated in RDF Turtle file (`out/population.ttl`)
- Uses [Pohyb obyvatel za ČR, kraje, okresy, SO ORP a obce - rok 2021](https://data.gov.cz/datov%C3%A1-sada?iri=https%3A%2F%2Fdata.gov.cz%2Fzdroj%2Fdatov%C3%A9-sady%2F00025593%2F12032e1445fd74fa08da79b14137fc29) dataset
- Uses dataset from care providers data cube to map counties to regions
- dimensions:
  - county
  - region
- measures:
  - mean population

### Integrity constraints
- Script `queries.py` checks data cube integrity constraints for both cubes
- Source of constraints: [The RDF Data Cube Vocabulary](https://www.w3.org/TR/vocab-data-cube/#h3_wf-rules)
- Output
  - `True` = Data cube violates corresponsing constraint
  - `False` = Data cube does not break this constraint
  - Ideally, all checks should return `False`

# Task 2
## System requirements
Python 3.10+ (tested on 3.10.5), Linux/WSL

## Installation instructions
1. Create a virtual environment and activate it 
(`python -m virtualenv venv`, `source venv/bin/activate`)
2. Install Apache Airflow using pip, following the official [instructions](https://airflow.apache.org/docs/apache-airflow/stable/start.html), or use Docker as an alternative.
3. Install required libraries (`pip install -r requirements.txt`)
4. Copy the content of the `airflow/dags` directory into your DAGs folder. Check `dags_folder` in `airflow.cfg`. (`cp -r airflow/dags/* <dags_folder>`)
5. Run the `data-cubes` DAG in Apache Airflow web interface. You can specify ouput directory using the "DAG with Config" option in Airflow. The format is `{"output_path": "./out"}`.

## Info
The structure of data cubes is identical to the previous task. 
However, the transformation workflow has been improved. And any incomplete values have been dropped, so there might be some minor differences compared to the previous cubes.

# Task 3
System requirements and installation instructions are the same as for Task 1.

Run `python provenance.py` to generate provenance file as  `out/provenance.trig`.

## Info
Resource names for each cube has been changed in task 1 code, so each has it's own unique one.
`NSR.dataCubeInstance -> NSR.careProvidersDataCubeInstance, NSR.populationDataCubeInstance`

# Task 4
System requirements and installation instructions are the same as for Task 1.

Run `python vocabs/skos_hierarchy.py` to generate SKOS hierarchy in  `out/skos_hierarchy.ttl`.
Run `python vocabs/dcat_dataset.py` to generate DCAT dataset for population datacube in  `out/dcat_dataset.ttl`.

## Info
I have decided to create a separate script to create SKOS hierarchy instead of adding it to cubes for improved readability.