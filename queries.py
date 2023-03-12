import rdflib

import cubes.care_providers as care_providers
import cubes.population as population

unique_dataset = """
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

unique_dsd = """
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

dsd_includes_measure = """
ASK {
  ?dsd a qb:DataStructureDefinition .
  FILTER NOT EXISTS { ?dsd qb:component [qb:componentProperty [a qb:MeasureProperty]] }
}
"""

dimensions_have_range = """
ASK {
  ?dim a qb:DimensionProperty .
  FILTER NOT EXISTS { ?dim rdfs:range [] }
}
"""

code_lists = """
ASK {
  ?dim a qb:DimensionProperty ;
       rdfs:range skos:Concept .
  FILTER NOT EXISTS { ?dim qb:codeList [] }
}
"""

attr_opt = """
ASK {
  ?dsd qb:component ?componentSpec .
  ?componentSpec qb:componentRequired "false"^^xsd:boolean ;
                 qb:componentProperty ?component .
  FILTER NOT EXISTS { ?component a qb:AttributeProperty }
}
"""

slice_keys = """
ASK {
    ?sliceKey a qb:SliceKey .
    FILTER NOT EXISTS { [a qb:DataStructureDefinition] qb:sliceKey ?sliceKey }
}
"""

slice_consistent = """
ASK {
  ?slicekey a qb:SliceKey;
      qb:componentProperty ?prop .
  ?dsd qb:sliceKey ?slicekey .
  FILTER NOT EXISTS { ?dsd qb:component [qb:componentProperty ?prop] }
}
"""

unique_slice = """
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

slice_dim = """
ASK {
  ?slice qb:sliceStructure [qb:componentProperty ?dim] .
  FILTER NOT EXISTS { ?slice ?dim [] }
}
"""

all_dim = """
ASK {
    ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .
    ?dim a qb:DimensionProperty;
    FILTER NOT EXISTS { ?obs ?dim [] }
}
"""

no_dup_obs = """
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

required_att = """
ASK {
    ?obs qb:dataSet/qb:structure/qb:component ?component .
    ?component qb:componentRequired "true"^^xsd:boolean ;
               qb:componentProperty ?attr .
    FILTER NOT EXISTS { ?obs ?attr [] }
}
"""

all_measures = """
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

measure_dim_consistent = """
ASK {
    # Observation in a measureType-cube
    ?obs qb:dataSet/qb:structure ?dsd ;
         qb:measureType ?measure .
    ?dsd qb:component/qb:componentProperty qb:measureType .
    # Must have value for its measureType
    FILTER NOT EXISTS { ?obs ?measure [] }
}
"""

single_measure = """
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

all_measures_present_in_measures = """
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

consistent_dataset_links = """
ASK {
    ?dataset qb:slice       ?slice .
    ?slice   qb:observation ?obs .
    FILTER NOT EXISTS { ?obs qb:dataSet ?dataset . }
}
"""

codes_from_code_lists_1 = """
ASK {
    ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .
    ?dim a qb:DimensionProperty ;
        qb:codeList ?list .
    ?list a skos:ConceptScheme .
    ?obs ?dim ?v .
    FILTER NOT EXISTS { ?v a skos:Concept ; skos:inScheme ?list }
}
"""

codes_from_code_lists_2 = """
ASK {
    ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .
    ?dim a qb:DimensionProperty ;
        qb:codeList ?list .
    ?list a skos:Collection .
    ?obs ?dim ?v .
    FILTER NOT EXISTS { ?v a skos:Concept . ?list skos:member+ ?v }
}
"""

codes_from_hierarchy = """
ASK {
    ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .
    ?dim a qb:DimensionProperty ;
        qb:codeList ?list .
    ?list a qb:HierarchicalCodeList .
    ?obs ?dim ?v .
    FILTER NOT EXISTS { ?list qb:hierarchyRoot/<$p>* ?v }
}
"""

codes_from_hierarchy_inv = """
ASK {
    ?obs qb:dataSet/qb:structure/qb:component/qb:componentProperty ?dim .
    ?dim a qb:DimensionProperty ;
         qb:codeList ?list .
    ?list a qb:HierarchicalCodeList .
    ?obs ?dim ?v .
    FILTER NOT EXISTS { ?list qb:hierarchyRoot/(^<$p>)* ?v }
}
"""

checks = {
    "Unique DataSet": unique_dataset,
    "Unique DSD": unique_dsd,
    "DSD includes measure": dsd_includes_measure,
    "Dimensions have range": dimensions_have_range,
    "Concept dimensions have code lists": code_lists,
    "Only attributes may be optional": attr_opt,
    # "Slice Keys must be declared":slice_keys, # this one throws error, but we dont use slices anyway
    "Slice Keys consistent with DSD": slice_consistent,
    "Unique slice structure": unique_slice,
    "Slice dimensions complete": slice_dim,
    "All dimensions required": all_dim,
    "No duplicate observations": no_dup_obs,
    "Required attributes": required_att,
    "All measures present": all_measures,
    "Measure dimension consistent": measure_dim_consistent,
    "Single measure on measure dimension observation": single_measure,
    "All measures present in measures dimension cube": all_measures_present_in_measures,
    "Consistent data set links": consistent_dataset_links,
    "Codes from code list 1": codes_from_code_lists_1,
    "Codes from code list 2": codes_from_code_lists_2,
    "Codes from hierarchy": codes_from_hierarchy,
    "Codes from hierarchy (inverse)": codes_from_hierarchy_inv
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

        print("True = constraint is broken")
        run_qb_check(cube, checks)
        print()


if __name__ == "__main__":
    main()
