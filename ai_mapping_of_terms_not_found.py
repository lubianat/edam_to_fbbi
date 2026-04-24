from openai import OpenAI
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

DEEP_SEEK_API_KEY = str(os.getenv("DEEP_SEEK_API_KEY"))

client = OpenAI(api_key=DEEP_SEEK_API_KEY, base_url="https://api.deepseek.com")

total_terms = ["edam", "fbbi", "wikidata"]

from itertools import combinations


# Give the content to AI and ask it to find exact matches between the entries on the two lists
# Cost <0.01 USD  per deepseek-chat call
import json

SYSTEM_PROMPT_TEMPLATE = """
You are a helpful assistant that finds exact matches between two lists of terms.
You will be given two lists of JSON objects for terms,
and you need to find exact matches between them.
The output should be a list of JSON objects, where each object has the following format:

{{
  "matches": [
    {{
      "{a}_id": "<id from List A>",
      "{a}_label": "<label from List A>",
      "{b}_id": "<id from List B>",
      "{b}_label": "<label from List B>"
    }}
  ]
}}
"""


def find_matches(names=("edam", "fbbi")):
    a, b = names
    df1 = pd.read_csv(f"output/terms/missing/{a}_terms_not_in_{b}.csv")
    df2 = pd.read_csv(f"output/terms/missing/{b}_terms_not_in_{a}.csv")

    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(a=a, b=b)
    user_prompt = (
        f"List A ({a}):\n{df1.to_json(orient='records')}\n\n"
        f"List B ({b}):\n{df2.to_json(orient='records')}"
    )

    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        response_format={"type": "json_object"},
        stream=False,
    )

    required_keys = {f"{a}_id", f"{a}_label", f"{b}_id", f"{b}_label"}
    valid_ids_a = set(df1["id"].astype(str))
    valid_ids_b = set(df2["id"].astype(str))

    try:
        payload = json.loads(response.choices[0].message.content)
        matches = payload.get("matches", [])
        if not isinstance(matches, list):
            raise ValueError("`matches` is not a list")

        clean = []
        for m in matches:
            if not isinstance(m, dict) or not required_keys.issubset(m):
                raise ValueError(f"Malformed match: {m}")
            # Drop any hallucinated ids that weren't in the inputs
            if str(m[f"{a}_id"]) not in valid_ids_a:
                continue
            if str(m[f"{b}_id"]) not in valid_ids_b:
                continue
            clean.append(m)
        return clean
    except Exception as e:
        print(f"Error parsing response: {e}")
        return []


# threading/multiprocessing

import concurrent.futures

# TIMES
TIMES = 3
print(f"Starting {TIMES} parallel runs...")

for combo in combinations(total_terms, 2):
    print(f"Finding matches between {combo[0]} and {combo[1]}...")

    df1 = pd.read_csv(f"output/terms/missing/{combo[0]}_terms_not_in_{combo[1]}.csv")
    df2 = pd.read_csv(f"output/terms/missing/{combo[1]}_terms_not_in_{combo[0]}.csv")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for _ in range(TIMES):
            futures.append(executor.submit(find_matches, names=combo))
        results = []
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.extend(result)

    # Collect non-empty runs
    non_empty_runs = [
        future.result()
        for future in concurrent.futures.as_completed(futures)
        if future.result()
    ]

    # Convert each match dict to a frozen tuple for set operations
    def match_to_tuple(m):
        return (
            m[f"{combo[0]}_id"],
            m[f"{combo[0]}_label"],
            m[f"{combo[1]}_id"],
            m[f"{combo[1]}_label"],
        )

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
        return {
            f"{combo[0]}_id": t[0],
            f"{combo[0]}_label": t[1],
            f"{combo[1]}_id": t[2],
            f"{combo[1]}_label": t[3],
        }

    intersection_matches = [tuple_to_dict(t) for t in intersection]
    all_matches = [tuple_to_dict(t) for t in all_unique]

    print(f"Intersection (all runs agree): {len(intersection_matches)} matches")
    print(f"Union (all unique found): {len(all_matches)} matches")

    pd.DataFrame(intersection_matches).to_csv(
        f"output/mappings/{combo[0]}/ai_all_{combo[0]}_{combo[1]}.csv",
        index=False,
    )

    pd.DataFrame(intersection_matches).to_csv(
        f"output/mappings/{combo[1]}/ai_all_{combo[0]}_{combo[1]}.csv",
        index=False,
    )
    # Unique not in intersection
    unique_not_in_intersection = all_unique - intersection
    unique_not_in_intersection_matches = [
        tuple_to_dict(t) for t in unique_not_in_intersection
    ]
    print(
        f"Unique matches not in intersection: {len(unique_not_in_intersection_matches)} matches"
    )

    # Save unique not in intersection matches to a separate file for manual review
    pd.DataFrame(unique_not_in_intersection_matches).to_csv(
        f"output/mappings/{combo[0]}/ai_single_{combo[0]}_{combo[1]}.csv",
        index=False,
    )
    pd.DataFrame(unique_not_in_intersection_matches).to_csv(
        f"output/mappings/{combo[1]}/ai_single_{combo[0]}_{combo[1]}.csv",
        index=False,
    )

    # Save total matches (at least 1 run) to a separate file for manual review
    pd.DataFrame(all_matches).to_csv(
        f"output/mappings/{combo[0]}/ai_all_unique_{combo[0]}_{combo[1]}.csv",
        index=False,
    )
    pd.DataFrame(all_matches).to_csv(
        f"output/mappings/{combo[1]}/ai_all_unique_{combo[0]}_{combo[1]}.csv",
        index=False,
    )

    # edam_terms_not_in_fbbi_after_ai_mapping.csv
    a_terms_not_in_b_after_ai = set(df1["id"].tolist()) - set(
        [match[f"{combo[0]}_id"] for match in all_matches]
    )
    a_terms_not_in_b_after_ai_df = df1[df1["id"].isin(a_terms_not_in_b_after_ai)]
    a_terms_not_in_b_after_ai_df.to_csv(
        f"output/terms/missing/{combo[0]}_terms_not_in_{combo[1]}_after_ai_mapping.csv",
        index=False,
    )

    # fbbi_terms_not_in_edam_after_ai_mapping.csv
    b_terms_not_in_a_after_ai = set(df2["id"].tolist()) - set(
        [match[f"{combo[1]}_id"] for match in all_matches]
    )
    b_terms_not_in_a_after_ai_df = df2[df2["id"].isin(b_terms_not_in_a_after_ai)]
    b_terms_not_in_a_after_ai_df.to_csv(
        f"output/terms/missing/{combo[1]}_terms_not_in_{combo[0]}_after_ai_mapping.csv",
        index=False,
    )
