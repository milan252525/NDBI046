import datetime
import os

import pandas as pd
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, QB, RDF, SKOS, XSD


def clean_population(file: str):
    data = pd.read_csv(file, low_memory=False)

    data = data[(data["vuk"] == "DEM0004") & (data["vuzemi_cis"] == 101)]

    columns = ["hodnota", "vuzemi_kod"]
    data = data[columns].dropna()

    data.to_csv(file)


NS = Namespace("https://milan252525.github.io/ontology#")
NSR = Namespace("https://milan252525.github.io/resources/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")

SDMX_DIM = Namespace("http://purl.org/linked-data/sdmx/2009/dimension#")
SDMX_CON = Namespace("http://purl.org/linked-data/sdmx/2009/concept#")
SDMX_MES = Namespace("http://purl.org/linked-data/sdmx/2009/measure#")


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

    return [county, region]


def _add_measures(cube: Graph) -> list[URIRef]:
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


def _create_resources(cube: Graph, enum_data: pd.DataFrame) -> None:
    for _, row in enum_data.iterrows():
        cube.add(
            (NSR[row["NUTS"]], SKOS.prefLabel, Literal(row["CountyName"], lang="cs"))
        )
        cube.add(
            (
                NSR[row["RegionCode"]],
                SKOS.prefLabel,
                Literal(row["RegionName"], lang="cs"),
            )
        )


def _create_observations(
    cube: Graph, dataset: URIRef, data: pd.DataFrame, enum_data: pd.DataFrame
) -> None:
    for index, row in data.iterrows():
        resource = NSR["observation-" + str(index).zfill(4)]
        cube.add((resource, RDF.type, QB.Observation))
        cube.add((resource, QB.dataSet, dataset))

        enum_row = enum_data.loc[enum_data["LAU"] == row["vuzemi_kod"]]

        cube.add((resource, NS.county, NSR[enum_row["NUTS"].values[0]]))
        cube.add((resource, NS.region, NSR[enum_row["RegionCode"].values[0]]))

        cube.add(
            (
                resource,
                NS.mean_population,
                Literal(row["hodnota"], datatype=XSD.integer),
            )
        )


def _create_datacube(data: pd.DataFrame, enum_data: pd.DataFrame) -> Graph:
    cube = Graph()
    dimensions = _add_dimensions(cube)
    measures = _add_measures(cube)
    structure = _create_structure(cube, dimensions, measures)
    dataset = _create_dataset(cube, structure)

    _create_resources(cube, enum_data)
    _create_observations(cube, dataset, data, enum_data)

    return cube


def create_population_datacube(data_file: str, enum_data: str, **kwargs):
    data = pd.read_csv(data_file)
    enum_data = pd.read_csv(enum_data)

    cube = _create_datacube(data, enum_data)

    output_path = kwargs["dag_run"].conf.get("output_path", "./out/")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    file_path = os.path.join(output_path, "population.ttl")
    with open(file_path, "wb") as file:
        cube.serialize(file, "ttl")
