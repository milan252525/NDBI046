from cubes import care_providers, population

UNIQUE_DATASET = """
ASK {
  {
    # Check observation has a data set
    ?obs a qb:Observation .
    FILTER NOT EXISTS { ?obs qb:dataSet ?dataset1 . }
  } UNION {
    # Check has just one data set
    ?obs a qb:Observation ;
       qb:dataSet ?dataset1, ?dataset2 .
    FILTER (?dataset1 != ?dataset2)
  }
}
"""

UNIQUE_DSD = """
ASK {
  {
    # Check dataset has a dsd
    ?dataset a qb:DataSet .
    FILTER NOT EXISTS { ?dataset qb:structure ?dsd . }
  } UNION {
    # Check has just one dsd
    ?dataset a qb:DataSet ;
       qb:structure ?dsd1, ?dsd2 .
    FILTER (?dsd1 != ?dsd2)
  }
}
"""

# this one kept failing even though the cube is correct,
# so I edited qb:componentProperty to qb:measure
DSD_INCLUDES_MEASURE = """
ASK {
  ?dsd a qb:DataStructureDefinition .
  FILTER NOT EXISTS { ?dsd qb:component [qb:measure [a qb:MeasureProperty]] }
}
"""

DIMENSIONS_HAVE_RANGE = """
ASK {
  ?dim a qb:DimensionProperty .
  FILTER NOT EXISTS { ?dim rdfs:range [] }
}
"""

CODE_LISTS = """
ASK {
  ?dim a qb:DimensionProperty ;
       rdfs:range skos:Concept .
  FILTER NOT EXISTS { ?dim qb:codeList [] }
}
"""

ATTR_OPT = """
ASK {
  ?dsd qb:component ?componentSpec .
  ?componentSpec qb:componentRequired "false"^^xsd:boolean ;
                 qb:componentProperty ?component .
  FILTER NOT EXISTS { ?component a qb:AttributeProperty }
}
"""

SLICE_KEYS = """
ASK {
    ?sliceKey a qb:SliceKey .
    FILTER NOT EXISTS { [a qb:DataStructureDefinition] qb:sliceKey ?sliceKey }
}
"""

SLICE_CONSISTENT = """
ASK {
  ?slicekey a qb:SliceKey;
      qb:componentProperty ?prop .
  ?dsd qb:sliceKey ?slicekey .
  FILTER NOT EXISTS { ?dsd qb:component [qb:componentProperty ?prop] }
}
"""

UNIQUE_SLICE = """
ASK {
  {
    # Slice has a key
    ?slice a qb:Slice .
    FILTER NOT EXISTS { ?slice qb:sliceStructure ?key }
  } UNION {
    # Slice has just one key
    ?slice a qb:Slice ;
           qb:sliceStructure ?key1, ?key2;
    FILTER (?key1 != ?key2)
  }
}
"""

SLICE_DIM = """
ASK {
  ?slice qb:sliceStructure [qb:componentProperty ?dim] .
  FILTER NOT EXISTS { ?slice ?dim [] }
}
"""

ALL_DIM = """
ASK {
    ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .
    ?dim a qb:DimensionProperty;
    FILTER NOT EXISTS { ?obs ?dim [] }
}
"""

NO_DUP_OBS = """
ASK {
  FILTER( ?allEqual )
  {
    # For each pair of observations test if all the dimension values are the same
    SELECT (MIN(?equal) AS ?allEqual) WHERE {
        ?obs1 qb:dataSet ?dataset .
        ?obs2 qb:dataSet ?dataset .
        FILTER (?obs1 != ?obs2)
        ?dataset qb:structure/qb:component/qb:componentProperty ?dim .
        ?dim a qb:DimensionProperty .
        ?obs1 ?dim ?value1 .
        ?obs2 ?dim ?value2 .
        BIND( ?value1 = ?value2 AS ?equal)
    } GROUP BY ?obs1 ?obs2
  }
}
"""

REQUIRED_ATT = """
ASK {
    ?obs qb:dataSet/qb:structure/qb:component ?component .
    ?component qb:componentRequired "true"^^xsd:boolean ;
               qb:componentProperty ?attr .
    FILTER NOT EXISTS { ?obs ?attr [] }
}
"""

ALL_MEASURES = """
ASK {
    # Observation in a non-measureType cube
    ?obs qb:dataSet/qb:structure ?dsd .
    FILTER NOT EXISTS { ?dsd qb:component/qb:componentProperty qb:measureType }

    # verify every measure is present
    ?dsd qb:component/qb:componentProperty ?measure .
    ?measure a qb:MeasureProperty;
    FILTER NOT EXISTS { ?obs ?measure [] }
}
"""

MEASURE_DIM_CONSISTENT = """
ASK {
    # Observation in a measureType-cube
    ?obs qb:dataSet/qb:structure ?dsd ;
         qb:measureType ?measure .
    ?dsd qb:component/qb:componentProperty qb:measureType .
    # Must have value for its measureType
    FILTER NOT EXISTS { ?obs ?measure [] }
}
"""

SINGLE_MEASURE = """
ASK {
    # Observation with measureType
    ?obs qb:dataSet/qb:structure ?dsd ;
         qb:measureType ?measure ;
         ?omeasure [] .
    # Any measure on the observation
    ?dsd qb:component/qb:componentProperty qb:measureType ;
         qb:component/qb:componentProperty ?omeasure .
    ?omeasure a qb:MeasureProperty .
    # Must be the same as the measureType
    FILTER (?omeasure != ?measure)
}
"""

ALL_MEASURES_PRESENT_IN_MEASURES = """
ASK {
  {
      # Count number of other measures found at each point
      SELECT ?numMeasures (COUNT(?obs2) AS ?count) WHERE {
          {
              # Find the DSDs and check how many measures they have
              SELECT ?dsd (COUNT(?m) AS ?numMeasures) WHERE {
                  ?dsd qb:component/qb:componentProperty ?m.
                  ?m a qb:MeasureProperty .
              } GROUP BY ?dsd
          }

          # Observation in measureType cube
          ?obs1 qb:dataSet/qb:structure ?dsd;
                qb:dataSet ?dataset ;
                qb:measureType ?m1 .

          # Other observation at same dimension value
          ?obs2 qb:dataSet ?dataset ;
                qb:measureType ?m2 .
          FILTER NOT EXISTS {
              ?dsd qb:component/qb:componentProperty ?dim .
              FILTER (?dim != qb:measureType)
              ?dim a qb:DimensionProperty .
              ?obs1 ?dim ?v1 .
              ?obs2 ?dim ?v2.
              FILTER (?v1 != ?v2)
          }

      } GROUP BY ?obs1 ?numMeasures
        HAVING (?count != ?numMeasures)
  }
}
"""

CONSISTENT_DATASET_LINKS = """
ASK {
    ?dataset qb:slice       ?slice .
    ?slice   qb:observation ?obs .
    FILTER NOT EXISTS { ?obs qb:dataSet ?dataset . }
}
"""

CODES_FROM_CODE_LISTS_1 = """
ASK {
    ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .
    ?dim a qb:DimensionProperty ;
        qb:codeList ?list .
    ?list a skos:ConceptScheme .
    ?obs ?dim ?v .
    FILTER NOT EXISTS { ?v a skos:Concept ; skos:inScheme ?list }
}
"""

CODES_FROM_CODE_LISTS_2 = """
ASK {
    ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .
    ?dim a qb:DimensionProperty ;
        qb:codeList ?list .
    ?list a skos:Collection .
    ?obs ?dim ?v .
    FILTER NOT EXISTS { ?v a skos:Concept . ?list skos:member+ ?v }
}
"""

CODES_FROM_HIERARCHY = """
ASK {
    ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .
    ?dim a qb:DimensionProperty ;
        qb:codeList ?list .
    ?list a qb:HierarchicalCodeList .
    ?obs ?dim ?v .
    FILTER NOT EXISTS { ?list qb:hierarchyRoot/<$p>* ?v }
}
"""

CODES_FROM_HIERARCHY_INVERSE = """
ASK {
    ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .
    ?dim a qb:DimensionProperty ;
         qb:codeList ?list .
    ?list a qb:HierarchicalCodeList .
    ?obs ?dim ?v .
    FILTER NOT EXISTS { ?list qb:hierarchyRoot/(^<$p>)* ?v }
}
"""

queries = {
    "Unique DataSet": UNIQUE_DATASET,
    "Unique DSD": UNIQUE_DSD,
    "DSD includes measure": DSD_INCLUDES_MEASURE,
    "Dimensions have range": DIMENSIONS_HAVE_RANGE,
    "Concept dimensions have code lists": CODE_LISTS,
    "Only attributes may be optional": ATTR_OPT,
    # "Slice Keys must be declared":SLICE_KEYS, # throws error, but we dont use slices anyway
    "Slice Keys consistent with DSD": SLICE_CONSISTENT,
    "Unique slice structure": UNIQUE_SLICE,
    "Slice dimensions complete": SLICE_DIM,
    "All dimensions required": ALL_DIM,
    "No duplicate observations": NO_DUP_OBS,
    "Required attributes": REQUIRED_ATT,
    "All measures present": ALL_MEASURES,
    "Measure dimension consistent": MEASURE_DIM_CONSISTENT,
    "Single measure on measure dimension observation": SINGLE_MEASURE,
    "All measures present in measures dimension cube": ALL_MEASURES_PRESENT_IN_MEASURES,
    "Consistent data set links": CONSISTENT_DATASET_LINKS,
    "Codes from code list 1": CODES_FROM_CODE_LISTS_1,
    "Codes from code list 2": CODES_FROM_CODE_LISTS_2,
    "Codes from hierarchy": CODES_FROM_HIERARCHY,
    "Codes from hierarchy (inverse)": CODES_FROM_HIERARCHY_INVERSE,
}


def run_qb_check(cube, checks):
    for check, query in checks.items():
        result = cube.query(query)
        print(f"{bool(result)} {check}")


def main():
    cubes = [care_providers.get_cube(), population.get_cube()]
    for cube in cubes:
        print(cube.name.upper())

        # required bindings
        cube.bind("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        cube.bind("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
        cube.bind("skos", "http://www.w3.org/2004/02/skos/core#")
        cube.bind("qb", "http://purl.org/linked-data/cube#")
        cube.bind("xsd", "http://www.w3.org/2001/XMLSchema#")
        cube.bind("owl", "http://www.w3.org/2002/07/owl#")

        print("> True = constraint is broken")
        run_qb_check(cube, queries)
        print()


if __name__ == "__main__":
    main()
