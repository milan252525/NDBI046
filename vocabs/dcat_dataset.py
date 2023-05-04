import datetime
import os

from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCAT, DCTERMS, RDF, XSD, FOAF

NS = Namespace("https://milan252525.github.io/ontology#")
NSR = Namespace("https://milan252525.github.io/resources/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")


def generate_dataset(graph: Graph) -> Graph:
    cube = NSR.populationDataCubeInstance

    graph.add((cube, RDF.type, DCAT.Dataset))
    graph.add((cube, DCTERMS.title, Literal("Population 2021", lang="en")))
    graph.add((cube, DCTERMS.title, Literal(
        "Obyvatelé v okresech 2021", lang="cs")))
    graph.add((cube, DCTERMS.description, Literal(
        "Datová kostka obsahující obyvatele podle krajů a okresů v roce 2021", lang="cs")))

    graph.add((cube, DCAT.keyword, Literal("populace", lang="cs")))
    graph.add((cube, DCAT.keyword, Literal("okresy", lang="cs")))
    graph.add((cube, DCAT.keyword, Literal("kraje", lang="cs")))

    # population statistics
    graph.add((cube, DCAT.theme, URIRef("http://eurovoc.europa.eu/4259")))
    # geographical distribution of the population
    graph.add((cube, DCAT.theme, URIRef("http://eurovoc.europa.eu/3300")))

    graph.add((cube, DCTERMS.spatial, URIRef(
        "http://publications.europa.eu/resource/authority/atu/CZE")))

    year = BNode()
    graph.add((cube, DCTERMS.temporal, year))
    graph.add((year, RDF.type, DCTERMS.PeriodOfTime))
    graph.add((year, DCAT.startDate, Literal(
        datetime.date(2021, 1, 1), datatype=XSD.date)))
    graph.add((year, DCAT.endDate, Literal(
        datetime.date(2021, 12, 31), datatype=XSD.date)))

    distribution = NSR.CubeDistribution
    graph.add((cube, DCAT.distribution, distribution))

    graph.add((distribution, RDF.type, DCAT.Distribution))
    graph.add((distribution, DCAT.accessURL, URIRef(
        "https://github.com/milan252525/NDBI046")))
    graph.add((distribution, DCAT.downloadURL, URIRef(
        "https://github.com/milan252525/NDBI046")))
    graph.add((distribution, DCAT.mediaType, URIRef(
        "http://www.iana.org/assignments/media-types/text/turtle")))
    graph.add((distribution, DCTERMS.format, URIRef(
        "http://publications.europa.eu/resource/authority/file-type/RDF_TURTLE")))

    publisher = BNode()
    graph.add((cube, DCTERMS.publisher, publisher))
    graph.add((cube, DCTERMS.creator, publisher))
    graph.add((publisher, RDF.type, FOAF.Person))
    graph.add((publisher, FOAF.firstName, Literal("Milan")))
    graph.add((publisher, FOAF.lastName, Literal("Abrahám")))

    graph.add((cube, DCTERMS.accrualPeriodicity, URIRef(
        "http://publications.europa.eu/resource/authority/frequency/IRREG")))

    return graph


def main() -> None:
    graph = Graph()

    graph = generate_dataset(graph)

    if not os.path.exists("out"):
        os.makedirs("out")
    with open("out/dcat_dataset.ttl", "wb") as file:
        graph.serialize(file, "ttl")
        print(f"Generated DCAT dataset into {file.name}")


if __name__ == "__main__":
    main()