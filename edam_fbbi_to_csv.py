import rdflib
import pandas as pd


g = rdflib.Graph()

g.parse(
    "https://github.com/edamontology/edam-bioimaging/raw/refs/heads/main/EDAM-bioimaging_dev.owl"
)

query = """
PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>
SELECT DISTINCT ?p WHERE {
    ?s oboInOwl:inSubset "bioimaging" .
    ?s  ?p ?a .
}
"""

results = g.query(query)
for row in results:
    print(row[0])

# http://www.geneontology.org/formats/oboInOwl#inSubset
# http://www.geneontology.org/formats/oboInOwl#hasDefinition
# http://www.geneontology.org/formats/oboInOwl#hasExactSynonym
# http://www.w3.org/2000/01/rdf-schema#comment
# http://www.w3.org/2000/01/rdf-schema#seeAlso
# http://www.w3.org/2000/01/rdf-schema#label


query = """
PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT DISTINCT
?id ?label
(GROUP_CONCAT(?exactSynonym; separator=", ") AS ?exactSynonyms)
(GROUP_CONCAT(?relatedSynonym; separator=", ") AS ?relatedSynonyms)
?definition
(GROUP_CONCAT(?comment; separator=", ") AS ?comments)
(GROUP_CONCAT(?seeAlso; separator=", ") AS ?seeAlsos)
?seeAlso
?wikipedia
WHERE {
    ?id rdfs:label ?label .
    ?id rdfs:subClassOf*  <http://edamontology.org/topic_3382> #  topic_3382 = Imaging
    OPTIONAL { ?id oboInOwl:hasExactSynonym ?exactSynonym . }
    OPTIONAL { ?id oboInOwl:relatedSynonym ?relatedSynonym . }
    OPTIONAL { ?id oboInOwl:hasDefinition ?definition . }
    OPTIONAL { ?id rdfs:comment ?comment . }
    OPTIONAL { ?id rdfs:seeAlso ?seeAlso . }
    OPTIONAL { ?id rdfs:seeAlso ?wikipedia .
               FILTER (STRSTARTS(STR(?wikipedia), "https://en.wikipedia.org/wiki/")) }
}
GROUP BY ?id ?label ?definition ?wikipedia
"""

results = g.query(query)
data = []
for row in results:
    data.append(
        {
            "subset": str(row[0]).replace("http://edamontology.org/", "").split("_")[0],
            "id": str(row[0]),
            "label": str(row[1]),
            "exactSynonyms": str(row[2]),
            "relatedSynonyms": str(row[3]),
            "definition": str(row[3]),
            "comments": str(row[4]),
            "seeAlsos": str(row[5]),
            "wikipedia": str(row[6]),
        }
    )
df = pd.DataFrame(data)

# None as empty string
df.to_csv("output/terms/edam_bioimaging.csv", index=False, na_rep="")

fbbi_graph = rdflib.Graph()
# download https://purl.obolibrary.org/obo/fbbi.owl
fbbi_graph.parse("fbbi.owl")

query = """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>
PREFIX FBbi: <http://purl.obolibrary.org/obo/FBbi_>
PREFIX obo: <http://purl.obolibrary.org/obo/>
SELECT ?id ?label
(GROUP_CONCAT(?exactSynonym; separator=", ") AS ?exactSynonyms)
(GROUP_CONCAT(?relatedSynonym; separator=", ") AS ?relatedSynonyms) ?definition WHERE {
    ?id rdfs:label ?label .
    OPTIONAL { ?id oboInOwl:hasExactSynonym ?exactSynonym . }
    OPTIONAL { ?id oboInOwl:relatedSynonym ?relatedSynonym . }
    OPTIONAL { ?id obo:IAO_0000115 ?definition . }
    ?id rdfs:subClassOf* FBbi:00000222 .
}
GROUP BY ?id ?label ?definition
"""
results = fbbi_graph.query(query)
fbbi_data = []
for row in results:
    fbbi_data.append(
        {
            "id": str(row[0]),
            "label": str(row[1]),
            "exactSynonyms": str(row[2]),
            "relatedSynonyms": str(row[3]),
            "definition": str(row[4]),
        }
    )
fbbi_df = pd.DataFrame(fbbi_data)
fbbi_df.drop_duplicates(inplace=True)
fbbi_df.to_csv("output/terms/fbbi_terms.csv", index=False, na_rep="")
