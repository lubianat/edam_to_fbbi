from openai import OpenAI
import os
from dotenv import load_dotenv

load_dotenv()

DEEP_SEEK_API_KEY = str(os.getenv("DEEP_SEEK_API_KEY"))

client = OpenAI(api_key=DEEP_SEEK_API_KEY, base_url="https://api.deepseek.com")

import pandas as pd

fbbi_terms_df = pd.read_csv("output/terms/fbbi_terms_not_in_edam.csv")
edam_terms_df = pd.read_csv("output/terms/edam_terms_not_in_fbbi.csv")

# Give the content to AI and ask it to find exact matches between the entries on the two lists
# Cost <0.01 USD  per deepseek-chat call


def find_matches(fbbi_terms_df, edam_terms_df):

    response = client.chat.completions.create(
        model="deepseek-chat",  # As of April 7, 2026 DeepSeek-V3.2 (Non-thinking Mode)
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant that finds exact matches between two lists of terms."
                "You will be given two lists of JSON objects for terms, "
                "and you need to find exact matches between them. "
                "The output should be a list of JSON objects, where each object has the following format: "
                "{'edam_id': 'EDAM_ID', 'edam_label': 'EDAM_LABEL', 'fbbi_id': 'FBBI_ID', 'fbbi_label': 'FBBI_LABEL'}",
            },
            {
                "role": "user",
                "content": f"Here are the two lists of terms: {fbbi_terms_df.to_json(orient='records')} and {edam_terms_df.to_json(orient='records')}",
            },
        ],
        stream=False,
    )
    # Validate that the response is a list of JSON objects with the correct format
    try:
        matches = eval(response.choices[0].message.content)
        if not isinstance(matches, list):
            raise ValueError("Response is not a list")
        for match in matches:
            if not isinstance(match, dict):
                raise ValueError("Match is not a JSON object")
            if not all(
                key in match
                for key in ["edam_id", "edam_label", "fbbi_id", "fbbi_label"]
            ):
                raise ValueError("Match does not have the correct keys")
    except Exception as e:
        print(f"Error parsing response: {e}")
        return []
    return matches


# threading/multiprocessing

import concurrent.futures

# TIMES
TIMES = 10
print(f"Starting {TIMES} parallel runs...")
with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = []
    for _ in range(TIMES):
        futures.append(executor.submit(find_matches, fbbi_terms_df, edam_terms_df))
    results = []
    for future in concurrent.futures.as_completed(futures):
        result = future.result()
        results.extend(result)

# get set intersection of all non-empty runs

# Collect non-empty runs
non_empty_runs = [
    future.result()
    for future in concurrent.futures.as_completed(futures)
    if future.result()
]


# Convert each match dict to a frozen tuple for set operations
def match_to_tuple(m):
    return (m["edam_id"], m["edam_label"], m["fbbi_id"], m["fbbi_label"])


# Output 1: Intersection of all non-empty runs
if non_empty_runs:
    sets = [set(match_to_tuple(m) for m in run) for run in non_empty_runs]
    intersection = sets[0]
    for s in sets[1:]:
        intersection &= s
else:
    intersection = set()

# Output 2: Union of all unique matches
all_unique = set(match_to_tuple(m) for m in results)


# Convert back to dicts
def tuple_to_dict(t):
    return {"edam_id": t[0], "edam_label": t[1], "fbbi_id": t[2], "fbbi_label": t[3]}


intersection_matches = [tuple_to_dict(t) for t in intersection]
all_matches = [tuple_to_dict(t) for t in all_unique]

print(f"Intersection (all runs agree): {len(intersection_matches)} matches")
print(f"Union (all unique found): {len(all_matches)} matches")

pd.DataFrame(intersection_matches).to_csv(
    "output/mappings/ai_mappings_intersection_matches.csv", index=False
)
pd.DataFrame(all_matches).to_csv(
    "output/mappings/ai_mappings_all_unique_matches.csv", index=False
)

# Unique not in intersection
unique_not_in_intersection = all_unique - intersection
unique_not_in_intersection_matches = [
    tuple_to_dict(t) for t in unique_not_in_intersection
]
print(
    f"Unique matches not in intersection: {len(unique_not_in_intersection_matches)} matches"
)
pd.DataFrame(unique_not_in_intersection_matches).to_csv(
    "output/mappings/ai_mappings_unique_not_in_intersection_matches.csv", index=False
)

# edam_terms_not_in_fbbi_after_ai_mapping.csv
edam_terms_not_in_fbbi_after_ai_mapping = set(edam_terms_df["id"].tolist()) - set(
    [match["edam_id"] for match in all_matches]
)
edam_terms_not_in_fbbi_after_ai_mapping_df = edam_terms_df[
    edam_terms_df["id"].isin(edam_terms_not_in_fbbi_after_ai_mapping)
]
edam_terms_not_in_fbbi_after_ai_mapping_df.to_csv(
    "output/terms/edam_terms_not_in_fbbi_after_ai_mapping.csv", index=False
)

# fbbi_terms_not_in_edam_after_ai_mapping.csv
fbbi_terms_not_in_edam_after_ai_mapping = set(fbbi_terms_df["id"].tolist()) - set(
    [match["fbbi_id"] for match in all_matches]
)
fbbi_terms_not_in_edam_after_ai_mapping_df = fbbi_terms_df[
    fbbi_terms_df["id"].isin(fbbi_terms_not_in_edam_after_ai_mapping)
]
fbbi_terms_not_in_edam_after_ai_mapping_df.to_csv(
    "output/terms/fbbi_terms_not_in_edam_after_ai_mapping.csv", index=False
)
