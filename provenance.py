import datetime
import os

from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, PROV, FOAF, DCTERMS, XSD

NSR = Namespace("https://milan252525.github.io/resources/")

def add_entities(prov: Graph) -> tuple[Graph, URIRef, URIRef]:
    # care providers data cube
    care = NSR.careProvidersDataCubeInstance
    prov.add((care, RDF.type, PROV.Entity))
    prov.add((care, DCTERMS.title, Literal("Care Providers Datacube", "cs")))
    prov.add((
        care,
        PROV.hadPrimarySource,
        URIRef("https://data.gov.cz/zdroj/datov%C3%A9-sady/00024341/aa4c99d9f1480cca59807389cf88d4dc")
    ))
    # open data source
    prov.add((
        URIRef("https://data.gov.cz/zdroj/datov%C3%A9-sady/00024341/aa4c99d9f1480cca59807389cf88d4dc"),
        RDF.type,
        PROV.Entity
    ))
    prov.add((
        URIRef("https://data.gov.cz/zdroj/datov%C3%A9-sady/00024341/aa4c99d9f1480cca59807389cf88d4dc"),
        DCTERMS.title,
        Literal("Dataset - Národní registr poskytovatelů zdravotních služeb", "cs")
    ))

    # population data cube
    pop = NSR.populationDataCubeInstance
    prov.add((pop, RDF.type, PROV.Entity))
    prov.add((pop, DCTERMS.title, Literal("Population 2021 Datacube", "cs")))
    prov.add((
        pop,
        PROV.hadPrimarySource,
        URIRef("https://data.gov.cz/zdroj/datov%C3%A9-sady/00025593/12032e1445fd74fa08da79b14137fc29")
    ))
    # open data source
    prov.add((
        URIRef("https://data.gov.cz/zdroj/datov%C3%A9-sady/00025593/12032e1445fd74fa08da79b14137fc29"),
        RDF.type,
        PROV.Entity,
    ))
    prov.add((
        URIRef("https://data.gov.cz/zdroj/datov%C3%A9-sady/00025593/12032e1445fd74fa08da79b14137fc29"),
        DCTERMS.title,
        Literal("Dataset - Pohyb obyvatel za ČR, kraje, okresy, SO ORP a obce - rok 2021", "cs"),
    ))

    return prov, care, pop


def add_agents(prov: Graph) -> tuple[Graph, URIRef, URIRef]:
    # MFF
    org = NSR.MFF
    prov.add((org, RDF.type, PROV.Agent))
    prov.add((org, RDF.type, PROV.Organization))
    prov.add((org, FOAF.name, Literal("MFF UK", "cs")))

    # Author
    author = NSR.MilanAbraham
    prov.add((author, RDF.type, PROV.Agent))
    prov.add((author, RDF.type, PROV.Person))
    prov.add((author, FOAF.firstName, Literal("Milan", "cs")))
    prov.add((author, FOAF.surname, Literal("Abrahám", "cs")))

    # org + author
    prov.add((author, PROV.actedOnBehalfOf, org))

    # Ariflow script
    airflow = NSR.ApacheAirflowDAG
    prov.add((airflow, RDF.type, PROV.Agent))
    prov.add((airflow, RDF.type, PROV.SoftwareAgent))
    prov.add((airflow, FOAF.name, Literal("Apache Airflow DAG", "cs")))

    return prov, author, airflow


def add_activities(prov: Graph, care: URIRef, pop: URIRef, author: URIRef, airflow: URIRef) -> Graph:
    # script author role
    role = NSR.ScriptAuthor
    prov.add((role, RDF.type, PROV.Role))
    prov.add((role, FOAF.name, Literal("Script Author", "cs")))

    # airflow dag run
    run = NSR.AirflowDAGRun
    prov.add((run, RDF.type, PROV.Activity))
    prov.add((run, PROV.generated, care))
    prov.add((run, PROV.generated, pop))
    prov.add((run, PROV.used, airflow))
    prov.add((run, PROV.startedAtTime, Literal(datetime.datetime(2023, 4, 10, 16, 0, 5), datatype=XSD.date)))
    prov.add((run, PROV.endedAtTime, Literal(datetime.datetime(2023, 4, 10, 16, 5), datatype=XSD.date)))

    #qualified usage
    usage = BNode()
    prov.add((usage, RDF.type, PROV.Usage))
    prov.add((usage, PROV.entity, author))
    prov.add((usage, PROV.hadRole, role))
    prov.add((usage, PROV.atTime,  Literal(datetime.datetime(2023, 4, 10, 16, 0), datatype=XSD.date)))

    prov.add((run, PROV.qualifiedUsage, usage))

    return prov


def create_provenance() -> Graph:
    prov = Graph()

    prov, data1, data2 = add_entities(prov)
    prov, author, script = add_agents(prov)
    prov = add_activities(prov, data1, data2, author, script)

    return prov


def main() -> None:
    prov = create_provenance()


    if not os.path.exists("out"):
        os.makedirs("out")
    with open("out/provenance.trig", "wb") as file:
        prov.serialize(file, "trig")
        print(f"Generated provenance into {file.name}")


if __name__ == "__main__":
    main()
