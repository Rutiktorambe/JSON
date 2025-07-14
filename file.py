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

def convert_value(val, dtype):
    if dtype == "number":
        try:
            return float(val)
        except:
            return val
    elif dtype == "boolean":
        return str(val).strip().lower() in ["true", "1", "yes"]
    elif dtype == "string":
        return str(val)
    elif dtype == "date":
        return str(val)
    return val

def insert_path_nested(d, path, key, value):
    """Insert a nested dictionary like insert_path_nested(obj, 'z/e', 'a', 'value') => obj['z']['e'] = {'a': value}"""
    keys = path.split('/')
    for k in keys[:-1]:
        d = d.setdefault(k, {})
    d[keys[-1]] = {key: value}

def insert_path_direct(d, path, value_dict):
    """Insert path like 'z/w/i' = {'f': value}"""
    keys = path.split('/')
    for k in keys[:-1]:
        d = d.setdefault(k, {})
    d[keys[-1]] = value_dict

def process_row(row, mappings):
    final = {}
    list_struct = defaultdict(lambda: defaultdict(dict))  # path -> index -> {prefix+var: value}

    for m in mappings:
        mtype, var, prefix, path, dtype = m.values()

        if mtype == "list":
            pattern = re.compile(rf"{prefix}(\d+)[_]?{var}$")
            for col in row.index:
                match = pattern.fullmatch(col)
                if match:
                    idx = int(match.group(1)) - 1
                    field_name = f"{prefix}{var}"
                    list_struct[path][idx][field_name] = convert_value(row[col], dtype)
        else:
            if var in row:
                value = convert_value(row[var], dtype)
                insert_path_nested(final, path, var, value)

    # Insert list values
    for path, index_dict in list_struct.items():
        list_items = []
        for idx in sorted(index_dict):
            list_items.append(index_dict[idx])
        insert_path_direct(final, path, list_items)

    return final

def process_csv_to_json(mapping_file, csv_file, output_file):
    mappings = read_mapping(mapping_file)
    df = pd.read_csv(csv_file)
    result = {"Quote": []}
    for _, row in df.iterrows():
        result["Quote"].append(process_row(row, mappings))

    with open(output_file, 'w') as f:
        json.dump(result, f, indent=4)
    print(f"✅ Output saved to {output_file}")

# Run it
# process_csv_to_json("working mapping.xlsm", "rrf.csv", "output.json")
process_csv_to_json("working mapping.xlsx", "rrf.csv", "output.json")