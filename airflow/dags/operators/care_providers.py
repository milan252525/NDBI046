import datetime
import os

import pandas as pd
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, QB, RDF, SKOS, XSD


def clean_care_providers(file: str):
    data = pd.read_csv(file, low_memory=False)

    columns = ["Okres", "OkresCode", "Kraj", "KrajCode", "OborPece"]
    data = data[columns].dropna()

    data.to_csv(file)


NS = Namespace("https://milan252525.github.io/ontology#")
NSR = Namespace("https://milan252525.github.io/resources/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")

SDMX_DIM = Namespace("http://purl.org/linked-data/sdmx/2009/dimension#")
SDMX_CON = Namespace("http://purl.org/linked-data/sdmx/2009/concept#")
SDMX_MES = Namespace("http://purl.org/linked-data/sdmx/2009/measure#")


def _create_datacube(data: pd.DataFrame) -> Graph:
    cube = Graph()
    dimensions = _add_dimensions(cube)
    measures = _add_measures(cube)
    structure = _create_structure(cube, dimensions, measures)
    dataset = _create_dataset(cube, structure)

    _create_resources(cube, data)
    _create_observations(cube, dataset, data)

    return cube


def _add_dimensions(cube: Graph) -> list[URIRef]:
    county = NS.county
    properties = [
        (RDF.type, RDFS.Property),
        (RDF.type, QB.DimensionProperty),
        (RDFS.label, Literal("Okres", lang="cs")),
        (RDFS.label, Literal("County", lang="en")),
        (RDFS.range, XSD.anyURI),
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
        (RDFS.range, XSD.anyURI),
        (RDFS.subPropertyOf, SDMX_DIM.refArea),
        (QB.concept, SDMX_CON.refArea),
    ]
    for prop in properties:
        cube.add((region, *prop))

    field_of_care = NS.field_of_care
    properties = [
        (RDF.type, RDFS.Property),
        (RDF.type, QB.DimensionProperty),
        (RDFS.label, Literal("Obor péče", lang="cs")),
        (RDFS.label, Literal("Field of care", lang="en")),
        (RDFS.range, XSD.anyURI),
        (RDFS.subPropertyOf, SDMX_DIM.coverageSector),
        (QB.concept, SDMX_CON.coverageSector),
    ]
    for prop in properties:
        cube.add((field_of_care, *prop))

    return [county, region, field_of_care]


def _add_measures(cube: Graph) -> list[URIRef]:
    number_of_care_providers = NS.number_of_care_providers

    properties = [
        (RDF.type, RDFS.Property),
        (RDF.type, QB.MeasureProperty),
        (RDFS.label, Literal("Počet poskytovatelů péče", lang="cs")),
        (RDFS.label, Literal("Number of care providers", lang="en")),
        (RDFS.range, XSD.integer),
        (RDFS.subPropertyOf, SDMX_MES.obsValue),
    ]
    for prop in properties:
        cube.add((number_of_care_providers, *prop))

    return [number_of_care_providers]


def _create_structure(
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


def _create_dataset(cube: Graph, structure: URIRef) -> URIRef:
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

    cube.add((dataset, DCTERMS.publisher, Literal("https://github.com/milan252525/")))
    cube.add(
        (
            dataset,
            DCTERMS.license,
            Literal("https://github.com/milan252525/NDBI046/blob/main/LICENSE"),
        )
    )

    return dataset


def _create_resources(cube: Graph, data: pd.DataFrame) -> None:
    for _, row in data[["Okres", "OkresCode"]].drop_duplicates().dropna().iterrows():
        cube.add(
            (
                NSR[row["OkresCode"]],
                SKOS.prefLabel,
                Literal(str(row["Okres"]), lang="cs"),
            )
        )

    for _, row in data[["Kraj", "KrajCode"]].drop_duplicates().dropna().iterrows():
        cube.add(
            (NSR[row["KrajCode"]], SKOS.prefLabel, Literal(str(row["Kraj"]), lang="cs"))
        )

    for _, row in data[["OborPece"]].drop_duplicates().dropna().iterrows():
        field = row["OborPece"].strip().replace(", ", ",").replace(" ", "_").lower()
        cube.add((NSR[field], SKOS.prefLabel, Literal(str(row["OborPece"]), lang="cs")))


def _create_observations(cube: Graph, dataset: URIRef, data: pd.DataFrame) -> None:
    data = data.groupby(["OkresCode", "KrajCode", "OborPece"])

    for index, ((county, region, field_of_care), group) in enumerate(data):
        resource = NSR["observation-" + str(index).zfill(4)]
        cube.add((resource, RDF.type, QB.Observation))
        cube.add((resource, QB.dataSet, dataset))
        cube.add((resource, QB.dataSet, dataset))
        cube.add((resource, NS.county, NSR[county]))
        cube.add((resource, NS.region, NSR[region]))
        field_of_care = (
            field_of_care.strip().replace(", ", ",").replace(" ", "_").lower()
        )
        cube.add((resource, NS.field_of_care, NSR[field_of_care]))
        cube.add(
            (
                resource,
                NS.number_of_care_providers,
                Literal(len(group), datatype=XSD.integer),
            )
        )


def create_care_providers_datacube(data_file: str, **kwargs):
    data = pd.read_csv(data_file)

    cube = _create_datacube(data)

    output_path = kwargs["dag_run"].conf.get("output_path", "./out/")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    file_path = os.path.join(output_path, "health_care.ttl")
    with open(file_path, "wb") as file:
        cube.serialize(file, "ttl")
