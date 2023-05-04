import os

import pandas as pd
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import SKOS, RDF

NS = Namespace("https://milan252525.github.io/ontology#")
NSR = Namespace("https://milan252525.github.io/resources/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")


def clean_care_providers(care_providers: pd.DataFrame) -> pd.DataFrame:
    columns = ["Okres", "OkresCode", "Kraj", "KrajCode", "OborPece"]
    care_providers = care_providers[columns].dropna()
    return care_providers


def create_hierarchy(graph: Graph) -> Graph:
    region = NS.region
    county = NS.county
    
    eurovoc_regauth = URIRef("http://eurovoc.europa.eu/6034")
    
    graph.add((eurovoc_regauth, RDF.type, SKOS.ConceptScheme))
    graph.add((eurovoc_regauth, SKOS.prefLabel, Literal("správní celek", lang="cs")))
    graph.add((eurovoc_regauth, SKOS.prefLabel, Literal("regional and local authorities", lang="en")))
    graph.add((eurovoc_regauth, SKOS.hasTopConcept, region))
    graph.add((eurovoc_regauth, SKOS.hasTopConcept, county))
    graph.add((eurovoc_regauth, SKOS.notation, Literal("6034")))

    graph.add((region, RDF.type, SKOS.ConceptScheme))
    graph.add((region, SKOS.inScheme, eurovoc_regauth))
    graph.add((region, SKOS.prefLabel, Literal("Kraj", lang="cs")))
    graph.add((region, SKOS.prefLabel, Literal("Region", lang="en")))

    graph.add((county, RDF.type, SKOS.ConceptScheme))
    graph.add((county, SKOS.inScheme, eurovoc_regauth))
    graph.add((county, SKOS.prefLabel, Literal("Okres", lang="cs")))
    graph.add((county, SKOS.prefLabel, Literal("County", lang="en")))

    return graph


def add_resources(data: pd.DataFrame, graph: Graph) -> Graph:
    region = NS.region
    county = NS.county

    for _, row in (
        data[["Okres", "OkresCode", "Kraj", "KrajCode"]]
        .drop_duplicates()
        .dropna()
        .iterrows()
    ):
        graph.add(
            (
                NSR[row["OkresCode"]],
                RDF.type,
                SKOS.Concept,
            )
        )
        graph.add(
            (
                NSR[row["OkresCode"]],
                SKOS.prefLabel,
                Literal(str(row["Okres"]), lang="cs"),
            )
        )
        graph.add(
            (
                NSR[row["OkresCode"]],
                SKOS.notation,
                Literal(str(row["OkresCode"])),
            )
        )
        graph.add((region, SKOS.hasTopConcept, NSR[row["OkresCode"]]))
        graph.add((NSR[row["OkresCode"]], SKOS.inScheme, region))

        graph.add(
            (
                NSR[row["KrajCode"]],
                RDF.type,
                SKOS.Concept,
            )
        )
        graph.add(
            (NSR[row["KrajCode"]], SKOS.prefLabel, Literal(str(row["Kraj"]), lang="cs"))
        )
        graph.add((NSR[row["KrajCode"]], SKOS.notation, Literal(str(row["KrajCode"]))))
        graph.add((county, SKOS.hasTopConcept, NSR[row["KrajCode"]]))
        graph.add((NSR[row["KrajCode"]], SKOS.inScheme, county))

        graph.add((NSR[row["KrajCode"]], SKOS.narrower, NSR[row["OkresCode"]]))
        graph.add((NSR[row["OkresCode"]], SKOS.broader, NSR[row["KrajCode"]]))

    return graph


def main() -> None:
    CP_URL = "https://opendata.mzcr.cz/data/nrpzs/narodni-registr-poskytovatelu-zdravotnich-sluzeb.csv"

    care_providers = pd.read_csv(CP_URL, low_memory=False)
    care_providers = clean_care_providers(care_providers)

    graph = Graph()
    graph = create_hierarchy(graph)
    graph = add_resources(care_providers, graph)

    if not os.path.exists("out"):
        os.makedirs("out")
    with open("out/skos_hierarchy.ttl", "wb") as file:
        graph.serialize(file, "ttl")
        print(f"Generated SKOS hierarchy into {file.name}")


if __name__ == "__main__":
    main()
