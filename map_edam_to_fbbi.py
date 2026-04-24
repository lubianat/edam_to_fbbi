from itertools import combinations

from numpy import save
import pandas as pd

edam_df = pd.read_csv("output/terms/edam_bioimaging_terms.csv")
fbbi_df = pd.read_csv("output/terms/fbbi_terms.csv")
wikidata_df = pd.read_csv("output/terms/wikidata_microscopy_terms.csv")
total_names = ["edam", "fbbi", "wikidata"]

# create directories
import os

for name in total_names:
    os.makedirs(f"output/mappings/{name}", exist_ok=True)


# exact_matches
# a 1:1 (case-insensitive) match between
def export_label_matches(names=("edam", "fbbi"), save=True):

    # use {name}_df to get the corresponding dataframe, e.g. edam_df or fbbi_df
    df1 = eval(f"{names[0]}_df")
    df2 = eval(f"{names[1]}_df")
    label_matches = []

    for i, row in df1.iterrows():
        for j, row2 in df2.iterrows():
            # if nan, skip
            if pd.isna(row["label"]) or pd.isna(row2["label"]):
                continue
            if row["label"].lower() == row2["label"].lower():
                label_matches.append(
                    {
                        f"{names[0]}_id": row["id"],
                        f"{names[0]}_label": row["label"],
                        f"{names[1]}_id": row2["id"],
                        f"{names[1]}_label": row2["label"],
                    }
                )

    joint_labels = pd.DataFrame(label_matches)

    if save:
        joint_labels.to_csv(
            f"output/mappings/{names[0]}/{names[0]}_label_{names[1]}_label_matches.csv",
            index=False,
        )
        joint_labels.to_csv(
            f"output/mappings/{names[1]}/{names[0]}_label_{names[1]}_label_matches.csv",
            index=False,
        )
    return joint_labels


def export_label_exact_synonym_matches(label_df="edam", synonym_df="fbbi", save=True):

    df1 = eval(f"{label_df}_df")
    df2 = eval(f"{synonym_df}_df")
    label_exact_synonym_matches = []

    for i, row in df1.iterrows():
        for j, row2 in df2.iterrows():

            if pd.isna(row["label"]) or pd.isna(row2["exactSynonyms"]):
                continue
            df1_label = row["label"].lower()

            df2_synonyms = row2["exactSynonyms"]
            # if not nan
            if pd.notna(df2_synonyms):
                df2_synonyms = [syn.strip().lower() for syn in df2_synonyms.split(",")]
                if df1_label in df2_synonyms:
                    label_exact_synonym_matches.append(
                        {
                            f"{label_df}_id": row["id"],
                            f"{label_df}_label": row["label"],
                            f"{synonym_df}_id": row2["id"],
                            f"{synonym_df}_label": row2["label"],
                        }
                    )
    label_exact_synonym_matches_df = pd.DataFrame(label_exact_synonym_matches)

    if save:
        label_exact_synonym_matches_df.to_csv(
            f"output/mappings/{label_df}/{label_df}_label_{synonym_df}_exact_synonym_matches.csv",
            index=False,
        )
        label_exact_synonym_matches_df.to_csv(
            f"output/mappings/{synonym_df}/{label_df}_label_{synonym_df}_exact_synonym_matches.csv",
            index=False,
        )
    return label_exact_synonym_matches_df


# fbbi exact synonym and edam exact synonym-level
def export_exact_synonym_matches(names=("edam", "fbbi"), save=True):
    df1 = eval(f"{names[0]}_df")
    df2 = eval(f"{names[1]}_df")
    synonym_matches = []

    for i, row in df1.iterrows():
        for j, row2 in df2.iterrows():

            df1_synonyms = row["exactSynonyms"]
            df2_synonyms = row2["exactSynonyms"]

            if pd.notna(df1_synonyms) and pd.notna(df2_synonyms):
                df1_synonyms = [syn.strip().lower() for syn in df1_synonyms.split(",")]
                df2_synonyms = [syn.strip().lower() for syn in df2_synonyms.split(",")]
                matching_synonyms = set(df1_synonyms) & set(df2_synonyms)

                if matching_synonyms:
                    synonym_matches.append(
                        {
                            f"{names[0]}_id": row["id"],
                            f"{names[0]}_label": row["label"],
                            f"{names[1]}_id": row2["id"],
                            f"{names[1]}_label": row2["label"],
                            "matching_synonym": ", ".join(matching_synonyms),
                        }
                    )
    joint_synonyms = pd.DataFrame(synonym_matches)

    if save:
        # save to both directories
        joint_synonyms.to_csv(
            f"output/mappings/{names[0]}/{names[0]}_exact_synonym_{names[1]}_exact_synonym_matches.csv",
            index=False,
        )
        joint_synonyms.to_csv(
            f"output/mappings/{names[1]}/{names[0]}_exact_synonym_{names[1]}_exact_synonym_matches.csv",
            index=False,
        )
    return joint_synonyms


for combination in combinations(total_names, 2):
    joint_labels = export_label_matches(combination, save=False)

    label_synonyms_1 = export_label_exact_synonym_matches(
        label_df=combination[0], synonym_df=combination[1], save=False
    )

    label_synonyms_2 = export_label_exact_synonym_matches(
        label_df=combination[1], synonym_df=combination[0], save=False
    )

    exact_synonyms = export_exact_synonym_matches(combination, save=False)

    theoretically_exact_matches = pd.concat(
        [
            joint_labels,
            label_synonyms_1,
            label_synonyms_2,
            exact_synonyms.drop(columns=["matching_synonym"]),
        ],
        ignore_index=True,
    )

    theoretically_exact_matches.drop_duplicates(inplace=True)
    theoretically_exact_matches.to_csv(
        f"output/mappings/{combination[0]}/{combination[0]}_{combination[1]}_exact_matches.csv",
        index=False,
    )
    theoretically_exact_matches.to_csv(
        f"output/mappings/{combination[1]}/{combination[0]}_{combination[1]}_exact_matches.csv",
        index=False,
    )

    terms_in_a_not_in_b = eval(f"{combination[0]}_df")[
        ~eval(f"{combination[0]}_df")["id"].isin(
            theoretically_exact_matches[f"{combination[0]}_id"]
        )
    ]
    terms_in_b_not_in_a = eval(f"{combination[1]}_df")[
        ~eval(f"{combination[1]}_df")["id"].isin(
            theoretically_exact_matches[f"{combination[1]}_id"]
        )
    ]

    os.makedirs("output/terms/missing", exist_ok=True)

    terms_in_a_not_in_b.to_csv(
        f"output/terms/missing/{combination[0]}_terms_not_in_{combination[1]}.csv",
        index=False,
    )
    terms_in_b_not_in_a.to_csv(
        f"output/terms/missing/{combination[1]}_terms_not_in_{combination[0]}.csv",
        index=False,
    )
