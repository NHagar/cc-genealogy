import os
from generate_dependency_mapping import pairs  # Import pairs

# Dependency mapping:
# This data is derived from /Users/nrh146/Documents/cc-genealogy/src/generate_dependency_mapping.py
# Ensuring syntactical correctness (e.g., commas between all tuple elements in the list).

# Absolute paths for robustness
base_path = "/Users/nrh146/Documents/cc-genealogy"
template_cc_path = os.path.join(base_path, "athena/kl_divergence_template_cc.sql")
template_hf_path = os.path.join(base_path, "athena/kl_divergence_template_hf.sql")
output_dir = os.path.join(base_path, "athena/generated")

try:
    with open(template_cc_path, "r") as f:
        template_cc_content_orig = f.read()
except FileNotFoundError:
    print(f"Error: Template file not found at {template_cc_path}")
    exit(1)
except IOError as e:
    print(f"Error reading template file {template_cc_path}: {e}")
    exit(1)

try:
    with open(template_hf_path, "r") as f:
        template_hf_content_orig = f.read()
except FileNotFoundError:
    print(f"Error: Template file not found at {template_hf_path}")
    exit(1)
except IOError as e:
    print(f"Error reading template file {template_hf_path}: {e}")
    exit(1)

# Prepare templates with distinct placeholders
# Assumes '({{ dataset_name }})' appears twice: first for p_base (left), second for q_base (right)
template_cc_script_version = template_cc_content_orig.replace(
    "({{ dataset_name }})", "({{ left_dataset_names }})", 1
)
template_cc_script_version = template_cc_script_version.replace(
    "({{ dataset_name }})", "({{ right_dataset_names }})", 1
)

template_hf_script_version = template_hf_content_orig.replace(
    "({{ dataset_name }})", "({{ left_dataset_names }})", 1
)
template_hf_script_version = template_hf_script_version.replace(
    "({{ dataset_name }})", "({{ right_dataset_names }})", 1
)

os.makedirs(output_dir, exist_ok=True)


def format_dataset_names_for_sql(dataset_list_or_str):
    if isinstance(dataset_list_or_str, str):
        datasets_raw = [dataset_list_or_str]
    else:
        datasets_raw = list(dataset_list_or_str)

    if not datasets_raw:
        return "", "unknown"

    stripped_datasets = [d.replace("nhagar/", "") for d in datasets_raw]

    valid_stripped_datasets = [d for d in stripped_datasets if d]
    if not valid_stripped_datasets:
        # This case might happen if a dataset name was just "nhagar/"
        return "", "unknown_empty_after_strip"

    sql_formatted_string = ",".join([f"'{d}'" for d in valid_stripped_datasets])
    first_stripped_name = valid_stripped_datasets[0]

    return sql_formatted_string, first_stripped_name


print(f"Starting query generation. Processing {len(pairs)} pairs.")

for i, pair_tuple in enumerate(pairs):
    if not (isinstance(pair_tuple, tuple) and len(pair_tuple) == 2):
        print(f"Skipping invalid pair at index {i}: {pair_tuple}")
        continue
    left_raw, right_raw = pair_tuple

    left_sql_str, first_left_stripped = format_dataset_names_for_sql(left_raw)
    right_sql_str, first_right_stripped = format_dataset_names_for_sql(right_raw)

    if (
        not left_sql_str
        or not right_sql_str
        or first_left_stripped == "unknown"
        or first_right_stripped == "unknown"
        or first_left_stripped == "unknown_empty_after_strip"
        or first_right_stripped == "unknown_empty_after_strip"
    ):
        print(
            f"Skipping pair {i + 1} due to empty or problematic dataset names after processing. Original: Left='{left_raw}', Right='{right_raw}'. Processed: Left='{first_left_stripped}', Right='{first_right_stripped}'"
        )
        continue

    chosen_template_str = ""
    current_left_list = left_raw
    if isinstance(left_raw, str):
        current_left_list = [left_raw]

    if (
        current_left_list
        and isinstance(current_left_list, list)
        and len(current_left_list) > 0
        and isinstance(current_left_list[0], str)
        and current_left_list[0].startswith("nhagar/CC-")
    ):
        chosen_template_str = template_cc_script_version
    else:
        chosen_template_str = template_hf_script_version

    populated_query = chosen_template_str.replace(
        "{{ left_dataset_names }}", left_sql_str
    )
    populated_query = populated_query.replace(
        "{{ right_dataset_names }}", right_sql_str
    )

    filename_lhs = (
        first_left_stripped.replace("/", "_").replace("-", "_").replace(".", "_")
    )
    filename_rhs = (
        first_right_stripped.replace("/", "_").replace("-", "_").replace(".", "_")
    )
    output_filename = f"kl_{filename_lhs}_vs_{filename_rhs}.sql"
    output_filepath = os.path.join(output_dir, output_filename)

    try:
        with open(output_filepath, "w") as f:
            f.write(populated_query)
        # print(f"Successfully saved: {output_filename}")
    except IOError as e:
        print(f"Error writing file {output_filepath}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while writing {output_filepath}: {e}")


print(f"Finished generating queries. Check the '{output_dir}' directory.")
