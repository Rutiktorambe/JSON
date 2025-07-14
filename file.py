import pandas as pd
import json
import re
from collections import defaultdict

def read_mapping(mapping_path):
    df = pd.read_excel(mapping_path)
    mappings = []
    for _, row in df.iterrows():
        mappings.append({
            'type': str(row['Type']).strip().lower(),
            'var': str(row['Variable']).strip(),
            'prefix': str(row['prefix']).strip(),
            'path': str(row['Path']).strip(),
            'datatype': str(row['DataType']).strip().lower()
        })
    return mappings

def get_default_value(dtype):
    if dtype == "string" or dtype == "date":
        return ""
    elif dtype == "number":
        return 0
    elif dtype == "boolean":
        return False
    return None

def convert_value(val, dtype):
    if pd.isna(val):
        return get_default_value(dtype)
    try:
        if dtype == "number":
            return float(val)
        elif dtype == "boolean":
            return str(val).strip().lower() in ["true", "1", "yes"]
        elif dtype in ["string", "date"]:
            return str(val)
    except:
        return get_default_value(dtype)
    return val

def insert_path_nested(d, path, key, value):
    keys = path.split('/')
    for k in keys[:-1]:
        d = d.setdefault(k, {})
    d[keys[-1]] = {key: value}

def insert_path_direct(d, path, value_dict):
    keys = path.split('/')
    for k in keys[:-1]:
        d = d.setdefault(k, {})
    d[keys[-1]] = value_dict

def process_row(row, mappings, all_headers):
    final = {}
    list_struct = defaultdict(lambda: defaultdict(dict))  # path -> index -> {prefix+var: value}

    # lowercase headers for case-insensitive lookup
    header_map = {col.lower(): col for col in all_headers}
    row_dict = {col.lower(): val for col, val in row.items()}

    list_field_max_index = defaultdict(int)

    for m in mappings:
        mtype, var, prefix, path, dtype = m.values()
        var_lc = var.lower()
        prefix_lc = prefix.lower()

        if mtype == "list":
            # match headers like pl_input1_b
            pattern = re.compile(rf"{prefix_lc}(\d+)[_]?{var_lc}$")
            matched_any = False
            for col_lc in header_map:
                match = pattern.fullmatch(col_lc)
                if match:
                    matched_any = True
                    idx = int(match.group(1)) - 1
                    field_name = f"{prefix}{var}"  # preserve original case in key
                    value = convert_value(row_dict.get(col_lc, None), dtype)
                    list_struct[path][idx][field_name] = value
                    list_field_max_index[path] = max(list_field_max_index[path], idx + 1)

            if not matched_any:
                # if no matching columns at all, still initialize one item
                list_struct[path][0][f"{prefix}{var}"] = get_default_value(dtype)
                list_field_max_index[path] = max(list_field_max_index[path], 1)

        else:
            # non-list field: match directly using lowercase
            if var_lc in row_dict:
                value = convert_value(row_dict[var_lc], dtype)
            else:
                value = get_default_value(dtype)
            insert_path_nested(final, path, var, value)

    # finalize list insertion
    for path, max_index in list_field_max_index.items():
        full_items = []
        for i in range(max_index):
            entry = list_struct[path].get(i, {})
            full_items.append(entry)
        insert_path_direct(final, path, full_items)

    return final

def process_csv_to_json(mapping_file, csv_file, output_file):
    mappings = read_mapping(mapping_file)
    df = pd.read_csv(csv_file)
    all_headers = list(df.columns)

    result = {"Quote": []}
    for _, row in df.iterrows():
        result["Quote"].append(process_row(row, mappings, all_headers))

    with open(output_file, 'w') as f:
        json.dump(result, f, indent=4)
    print(f"✅ Output saved to {output_file}")

# Run this like:
# process_csv_to_json("working mapping.xlsm", "rrf.csv", "output.json")
process_csv_to_json("working mapping.xlsx", "rrf.csv", "output.json")