# NDBI046 Introduction to Data Engineering
https://skoda.projekty.ms.mff.cuni.cz/ndbi046/

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