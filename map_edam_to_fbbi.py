import pandas as pd


edam_df = pd.read_csv("output/terms/edam_bioimaging.csv")
fbbi_df = pd.read_csv("output/terms/fbbi_terms.csv")

# exact_matches
# a 1:1 (case-insensitive) match between

# label-level

label_matches = []

for i, row in edam_df.iterrows():
    for j, row2 in fbbi_df.iterrows():
        if row["label"].lower() == row2["label"].lower():
            label_matches.append(
                {
                    "edam_id": row["id"],
                    "edam_label": row["label"],
                    "fbbi_id": row2["id"],
                    "fbbi_label": row2["label"],
                }
            )

joint_labels = pd.DataFrame(label_matches)

joint_labels.to_csv("output/mappings/edam_fbbi_label_matches.csv", index=False)


# EDAM label and exact FBbi synonym-level

edam_label_exact_fbbi_synonym_matches = []

for i, row in edam_df.iterrows():
    for j, row2 in fbbi_df.iterrows():
        edam_label = row["label"].lower()

        fbbi_synonyms = row2["exactSynonyms"]
        # if not nan
        if pd.notna(fbbi_synonyms):
            fbbi_synonyms = [syn.strip().lower() for syn in fbbi_synonyms.split(",")]
            if edam_label in fbbi_synonyms:
                edam_label_exact_fbbi_synonym_matches.append(
                    {
                        "edam_id": row["id"],
                        "edam_label": row["label"],
                        "fbbi_id": row2["id"],
                        "fbbi_label": row2["label"],
                    }
                )
edam_label_exact_fbbi_synonym_matches_df = pd.DataFrame(
    edam_label_exact_fbbi_synonym_matches
)
edam_label_exact_fbbi_synonym_matches_df.to_csv(
    "output/mappings/edam_label_fbbi_exact_synonym_matches.csv", index=False
)

# fbbi label and edam exact synonym-level

fbbi_label_exact_edam_synonym_matches = []

for i, row in edam_df.iterrows():
    for j, row2 in fbbi_df.iterrows():
        fbbi_label = row2["label"].lower()

        edam_synonyms = row["exactSynonyms"]
        # if not nan
        if pd.notna(edam_synonyms):
            edam_synonyms = [syn.strip().lower() for syn in edam_synonyms.split(",")]
            if fbbi_label in edam_synonyms:
                fbbi_label_exact_edam_synonym_matches.append(
                    {
                        "edam_id": row["id"],
                        "edam_label": row["label"],
                        "fbbi_id": row2["id"],
                        "fbbi_label": row2["label"],
                    }
                )
fbbi_label_exact_edam_synonym_matches_df = pd.DataFrame(
    fbbi_label_exact_edam_synonym_matches
)
fbbi_label_exact_edam_synonym_matches_df.to_csv(
    "output/mappings/edam_exact_synonym_fbbi_label_matches.csv", index=False
)

# fbbi exact synonym and edam exact synonym-level

synonym_matches = []

for i, row in edam_df.iterrows():
    for j, row2 in fbbi_df.iterrows():

        edam_synonyms = row["exactSynonyms"]
        fbbi_synonyms = row2["exactSynonyms"]

        if pd.notna(edam_synonyms) and pd.notna(fbbi_synonyms):
            edam_synonyms = [syn.strip().lower() for syn in edam_synonyms.split(",")]
            fbbi_synonyms = [syn.strip().lower() for syn in fbbi_synonyms.split(",")]
            matching_synonyms = set(edam_synonyms) & set(fbbi_synonyms)

            if matching_synonyms:
                synonym_matches.append(
                    {
                        "edam_id": row["id"],
                        "edam_label": row["label"],
                        "fbbi_id": row2["id"],
                        "fbbi_label": row2["label"],
                        "matching_synonym": ", ".join(matching_synonyms),
                    }
                )
joint_synonyms = pd.DataFrame(synonym_matches)
joint_synonyms.to_csv(
    "output/mappings/edam_exact_synonym_fbbi_exact_synonym_matches.csv", index=False
)

# Some exact match (label or exact synonym), dropping "match_synonym"

theoretically_exact_matches = pd.concat(
    [
        joint_labels,
        edam_label_exact_fbbi_synonym_matches_df,
        fbbi_label_exact_edam_synonym_matches_df,
        joint_synonyms.drop(columns=["matching_synonym"]),
    ],
    ignore_index=True,
)
theoretically_exact_matches.drop_duplicates(inplace=True)
theoretically_exact_matches.to_csv(
    "output/mappings/edam_fbbi_exact_matches.csv", index=False
)

terms_in_fbbi_not_in_edam = fbbi_df[
    ~fbbi_df["id"].isin(theoretically_exact_matches["fbbi_id"])
]
terms_in_edam_not_in_fbbi = edam_df[
    ~edam_df["id"].isin(theoretically_exact_matches["edam_id"])
]

terms_in_fbbi_not_in_edam.to_csv("output/terms/fbbi_terms_not_in_edam.csv", index=False)
terms_in_edam_not_in_fbbi.to_csv("output/terms/edam_terms_not_in_fbbi.csv", index=False)
