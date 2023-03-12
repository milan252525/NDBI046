import datetime
import os

import pandas as pd
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, QB, RDF, SKOS, XSD

SOURCE_CARE_PROVIDERS = "data/narodni-registr-poskytovatelu-zdravotnich-sluzeb.csv"

NS = Namespace("https://milan252525.github.io/ontology#")
NSR = Namespace("https://milan252525.github.io/resources/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")

SDMX_DIM = Namespace("http://purl.org/linked-data/sdmx/2009/dimension#")
SDMX_CON = Namespace("http://purl.org/linked-data/sdmx/2009/concept#")
SDMX_MES = Namespace("http://purl.org/linked-data/sdmx/2009/measure#")

COUNTY = "Okres"
COUNTY_CODE = "OkresCode"
REGION = "Kraj"
REGION_CODE = "KrajCode"
FIELD_OF_CARE = "OborPece"


def load_data() -> pd.DataFrame:
    # low_memory because the data has variable data types in columns
    return pd.read_csv(SOURCE_CARE_PROVIDERS, low_memory=False)


def create_datacube(data: pd.DataFrame) -> Graph:
    cube = Graph()
    dimensions = add_dimensions(cube)
    measures = add_measures(cube)
    structure = create_structure(cube, dimensions, measures)
    dataset = create_dataset(cube, structure)

    create_resources(cube, data)
    create_observations(cube, dataset, data.groupby(
        [COUNTY_CODE, REGION_CODE, FIELD_OF_CARE]))

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
    for p in properties:
        cube.add((county, *p))

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
    for p in properties:
        cube.add((region, *p))

    field_of_care = NS.field_of_care
    properties = [
        (RDF.type, RDFS.Property),
        (RDF.type, QB.DimensionProperty),
        (RDFS.label, Literal("Obor péče", lang="cs")),
        (RDFS.label, Literal("Field of care", lang="en")),
        (RDFS.range, XSD.string),
        (RDFS.subPropertyOf, SDMX_DIM.coverageSector),
        (QB.concept, SDMX_CON.coverageSector),
    ]
    for p in properties:
        cube.add((field_of_care, *p))

    return [county, region, field_of_care]


def add_measures(cube: Graph) -> list[URIRef]:
    number_of_care_providers = NS.number_of_care_providers

    properties = [
        (RDF.type, RDFS.Property),
        (RDF.type, QB.MeasureProperty),
        (RDFS.label, Literal("Počet poskytovatelů péče", lang="cs")),
        (RDFS.label, Literal("Number of care providers", lang="en")),
        (RDFS.range, XSD.integer),
        (RDFS.subPropertyOf, SDMX_MES.obsValue),
    ]
    for p in properties:
        cube.add((number_of_care_providers, *p))

    return [number_of_care_providers]


def create_structure(cube: Graph, dimensions: list[URIRef], measures: list[URIRef]) -> URIRef:
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
    cube.add((dataset, RDFS.label, Literal("Care providers", lang="en")))
    cube.add(
        (dataset, RDFS.label, Literal("Poskytovatelé zdravotních služeb", lang="cs"))
    )
    cube.add((dataset, QB.structure, structure))

    issued = datetime.date(2023, 3, 11)
    curr_date = datetime.date.today().isoformat()
    cube.add((dataset, DCTERMS.issued, Literal(issued, datatype=XSD.date)))
    cube.add((dataset, DCTERMS.modified, Literal(curr_date, datatype=XSD.date)))

    cube.add((dataset, DCTERMS.publisher, Literal(
        "https://github.com/milan252525/")))
    cube.add(
        (
            dataset,
            DCTERMS.license,
            Literal("https://github.com/milan252525/NDBI046/blob/main/LICENSE"),
        )
    )

    return dataset


def serialize_to_string(obj: any) -> str:
    return str(obj).strip().replace(", ", ",").replace(" ", "_").lower()


def create_resources(cube: Graph, data: pd.DataFrame) -> None:
    for _, row in data[[COUNTY, COUNTY_CODE]].drop_duplicates().dropna().iterrows():
        county = serialize_to_string(row[COUNTY_CODE])
        cube.add((NSR[county], RDF.type, NS.county))
        cube.add((NSR[county], SKOS.prefLabel,
                 Literal(str(row[COUNTY]), lang="cs")))

    for _, row in data[[REGION, REGION_CODE]].drop_duplicates().dropna().iterrows():
        region = serialize_to_string(row[REGION_CODE])
        cube.add((NSR[region], RDF.type, NS.region))
        cube.add((NSR[region], SKOS.prefLabel,
                 Literal(str(row[REGION]), lang="cs")))

    for _, row in data[[FIELD_OF_CARE]].drop_duplicates().dropna().iterrows():
        field = serialize_to_string(row[FIELD_OF_CARE])
        cube.add((NSR[field], RDF.type, NS.field_of_care))
        cube.add((NSR[field], SKOS.prefLabel, Literal(
            str(row[FIELD_OF_CARE]), lang="cs")))


def create_observations(cube: Graph, dataset: URIRef, data: pd.DataFrame) -> None:
    for index, ((county, region, field_of_care), group) in enumerate(data):
        resource = NSR["observation-" + str(index).zfill(4)]
        cube.add((resource, RDF.type, QB.Observation))
        cube.add((resource, QB.dataSet, dataset))
        cube.add((resource, QB.dataSet, dataset))
        cube.add((resource, NS.county, NSR[serialize_to_string(county)]))
        cube.add((resource, NS.region, NSR[serialize_to_string(region)]))
        cube.add(
            (resource, NS.field_of_care,
             NSR[serialize_to_string(field_of_care)])
        )
        cube.add(
            (resource, NS.number_of_care_providers,
             Literal(len(group), datatype=XSD.integer))
        )


def get_cube():
    cube = create_datacube(load_data())
    setattr(cube, "name", "Care providers")
    cube.bind("qb", QB)
    cube.bind("skos", SKOS)
    return cube


def main():
    print(f"Generating Care providers data cube")
    data = load_data()
    print(f"Dataset size: {len(data)}")
    cube = create_datacube(data)
    if not os.path.exists("out"):
        os.makedirs("out")
    with open("out/care_providers.ttl", "wb") as f:
        cube.serialize(f, "ttl")
        print(f"Generated data cube into {f.name}")


if __name__ == "__main__":
    main()
