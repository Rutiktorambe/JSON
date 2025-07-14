import pandas as pd
import json
import re
from collections import defaultdict

def read_mapping(mapping_path):
    df = pd.read_excel(mapping_path)
    mappings = []
    for _, row in df.iterrows():
        mappings.append({
            'type': str(row.get('Type', '')).strip().lower(),
            'var': str(row['Variable']).strip(),
            'prefix': str(row['prefix']).strip(),
            'path': str(row['Path']).strip(),
            'datatype': str(row['DataType']).strip().lower(),
            'samed': str(row.get('samed', '')).strip()
        })
    return mappings

def get_default_value(dtype):
    return {
        "string": "",
        "date": "",
        "number": 0,
        "boolean": False
    }.get(dtype, None)

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
    list_struct = defaultdict(lambda: defaultdict(dict))

    # Lowercase headers for lookup
    header_map = {col.lower(): col for col in all_headers}
    row_dict = {col.lower(): val for col, val in row.items()}

    list_field_max_index = defaultdict(int)

    for m in mappings:
        mtype, var, prefix, path, dtype, samed = m.values()
        var_lc = var.lower()
        prefix_lc = prefix.lower()
        samed_lc = samed.lower() if samed else ""

        if mtype == "list":
            # e.g., pl_input1_b or fallback plinputB_alt1
            pattern_primary = re.compile(rf"{prefix_lc}(\d+)[_]?{var_lc}$")
            pattern_fallback = re.compile(rf"{samed_lc}(\d+)$") if samed_lc else None
            matched = False

            for col_lc in header_map:
                match = pattern_primary.fullmatch(col_lc)
                if not match and pattern_fallback:
                    match = pattern_fallback.fullmatch(col_lc)

                if match:
                    idx = int(match.group(1)) - 1
                    field_name = f"{prefix}{var}"
                    value = convert_value(row_dict.get(col_lc, None), dtype)
                    list_struct[path][idx][field_name] = value
                    list_field_max_index[path] = max(list_field_max_index[path], idx + 1)
                    matched = True

            if not matched:
                list_struct[path][0][f"{prefix}{var}"] = get_default_value(dtype)
                list_field_max_index[path] = max(list_field_max_index[path], 1)

        else:
            # Non-list fields
            value = None
            if var_lc in row_dict:
                value = convert_value(row_dict[var_lc], dtype)
            elif samed_lc and samed_lc in row_dict:
                value = convert_value(row_dict[samed_lc], dtype)
            else:
                value = get_default_value(dtype)
            insert_path_nested(final, path, var, value)

    # Finalize list items
    for path, max_index in list_field_max_index.items():
        items = []
        for i in range(max_index):
            items.append(list_struct[path].get(i, {}))
        insert_path_direct(final, path, items)

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



process_csv_to_json("working mapping.xlsx", "rrf.csv", "output.json")