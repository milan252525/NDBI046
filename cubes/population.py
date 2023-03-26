import datetime
import os

import pandas as pd
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, QB, RDF, SKOS, XSD

SOURCE_POPULATION = "data/130141-22data2021.csv"
SOURCE_CARE_PROVIDERS = "data/narodni-registr-poskytovatelu-zdravotnich-sluzeb.csv"
COUNTY_CODELIST = "data/číselník-okresů-vazba-101-nadřízený.csv"

NS = Namespace("https://milan252525.github.io/ontology#")
NSR = Namespace("https://milan252525.github.io/resources/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")

SDMX_DIM = Namespace("http://purl.org/linked-data/sdmx/2009/dimension#")
SDMX_CON = Namespace("http://purl.org/linked-data/sdmx/2009/concept#")
SDMX_MES = Namespace("http://purl.org/linked-data/sdmx/2009/measure#")

COUNTY = "Okres"
REGION = "Kraj"


def load_data() -> pd.DataFrame:
    return pd.read_csv(SOURCE_POPULATION)


def load_care_providers() -> pd.DataFrame:
    return pd.read_csv(SOURCE_CARE_PROVIDERS, low_memory=False)


def load_codelist() -> pd.DataFrame:
    return pd.read_csv(COUNTY_CODELIST)


def create_datacube(data: pd.DataFrame, codelist: pd.DataFrame) -> Graph:
    cube = Graph()
    dimensions = add_dimensions(cube)
    measures = add_measures(cube)
    structure = create_structure(cube, dimensions, measures)
    dataset = create_dataset(cube, structure)

    # filter only mean population in counties
    data = data[(data["vuk"] == "DEM0004") & (data["vuzemi_cis"] == 101)]

    create_resources(cube, data, codelist)
    create_observations(cube, dataset, data, codelist)

    return cube


def add_dimensions(cube: Graph) -> list[URIRef]:
    county = NS.county
    properties = [
        (RDF.type, RDFS.Property),
        (RDF.type, QB.DimensionProperty),
        (RDFS.label, Literal("Okres", lang="cs")),
        (RDFS.label, Literal("County", lang="en")),
        (RDFS.range, XSD.string),
        (RDFS.subPropertyOf, SDMX_DIM.refArea),
        (QB.concept, SDMX_CON.refArea),
    ]
    for prop in properties:
        cube.add((county, *prop))

    region = NS.region
    properties = [
        (RDF.type, RDFS.Property),
        (RDF.type, QB.DimensionProperty),
        (RDFS.label, Literal("Kraj", lang="cs")),
        (RDFS.label, Literal("Region", lang="en")),
        (RDFS.range, XSD.string),
        (RDFS.subPropertyOf, SDMX_DIM.refArea),
        (QB.concept, SDMX_CON.refArea),
    ]
    for prop in properties:
        cube.add((region, *prop))

    return [county, region]


def add_measures(cube: Graph) -> list[URIRef]:
    mean_population = NS.mean_population

    properties = [
        (RDF.type, RDFS.Property),
        (RDF.type, QB.MeasureProperty),
        (RDFS.label, Literal("Střední stav obyvatel", lang="cs")),
        (RDFS.label, Literal("Mean population", lang="en")),
        (RDFS.range, XSD.integer),
        (RDFS.subPropertyOf, SDMX_MES.obsValue),
    ]
    for prop in properties:
        cube.add((mean_population, *prop))

    return [mean_population]


def create_structure(
    cube: Graph, dimensions: list[URIRef], measures: list[URIRef]
) -> URIRef:
    structure = NS.structure
    cube.add((structure, RDF.type, QB.DataStructureDefinition))

    for dimension in dimensions:
        component = BNode()
        cube.add((structure, QB.component, component))
        cube.add((component, QB.dimension, dimension))

    for measure in measures:
        component = BNode()
        cube.add((structure, QB.component, component))
        cube.add((component, QB.measure, measure))

    return structure


def create_dataset(cube: Graph, structure: URIRef) -> URIRef:
    dataset = NSR.dataCubeInstance
    cube.add((dataset, RDF.type, QB.DataSet))
    cube.add((dataset, RDFS.label, Literal("Population 2021", lang="en")))
    cube.add((dataset, RDFS.label, Literal("Obyvatelé v okresech 2021", lang="cs")))
    cube.add((dataset, QB.structure, structure))

    issued = datetime.date(2023, 3, 12)
    curr_date = datetime.date.today().isoformat()
    cube.add((dataset, DCTERMS.issued, Literal(issued, datatype=XSD.date)))
    cube.add((dataset, DCTERMS.modified, Literal(curr_date, datatype=XSD.date)))

    cube.add((dataset, DCTERMS.publisher, Literal("https://github.com/milan252525/")))
    cube.add(
        (
            dataset,
            DCTERMS.license,
            Literal("https://github.com/milan252525/NDBI046/blob/main/LICENSE"),
        )
    )

    return dataset


def translate_county_code_name(code: int, codelist: pd.DataFrame) -> str:
    return codelist[(codelist["CHODNOTA2"] == code)].iloc[0]["CHODNOTA1"]


def create_resources(cube: Graph, data: pd.DataFrame, codelist: pd.DataFrame) -> None:
    for _, row in data.iterrows():
        code = translate_county_code_name(row.vuzemi_kod, codelist)
        cube.add((NSR[code], SKOS.prefLabel, Literal(row.vuzemi_txt, lang="cs")))

    regions = load_care_providers()
    for _, row in regions[["KrajCode", "Kraj"]].drop_duplicates().dropna().iterrows():
        region = row["KrajCode"]
        cube.add((NSR[region], SKOS.prefLabel, Literal(row["Kraj"], lang="cs")))


def create_observations(
    cube: Graph, dataset: URIRef, data: pd.DataFrame, codelist: pd.DataFrame
) -> None:
    regions = load_care_providers()
    code_map = {}
    for _, row in (
        regions[["KrajCode", "OkresCode"]].drop_duplicates().dropna().iterrows()
    ):
        code_map[row["OkresCode"]] = row["KrajCode"]

    for index, row in data.iterrows():
        resource = NSR["observation-" + str(index).zfill(4)]
        cube.add((resource, RDF.type, QB.Observation))
        cube.add((resource, QB.dataSet, dataset))
        cube.add((resource, QB.dataSet, dataset))

        county = translate_county_code_name(row.vuzemi_kod, codelist)
        cube.add((resource, NS.county, NSR[county]))
        cube.add((resource, NS.region, NSR[code_map[county]]))

        cube.add(
            (resource, NS.mean_population, Literal(row.hodnota, datatype=XSD.integer))
        )


def get_cube():
    cube = create_datacube(load_data(), load_codelist())
    setattr(cube, "name", "Population 2021")
    cube.bind("qb", QB)
    cube.bind("skos", SKOS)
    return cube


def main():
    print("Generating Population 2021 data cube")
    data = load_data()
    codelist = load_codelist()
    print(f"Dataset size: {len(data)}")
    cube = create_datacube(data, codelist)
    if not os.path.exists("out"):
        os.makedirs("out")
    with open("out/population.ttl", "wb") as file:
        cube.serialize(file, "ttl")
        print(f"Generated data cube into {file.name}")


if __name__ == "__main__":
    main()
