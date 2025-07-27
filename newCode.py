import pandas as pd
import json
import re
from collections import defaultdict

def read_mapping(mapping_path):
    df = pd.read_excel(mapping_path)
    mappings = []
    for _, row in df.iterrows():
        prefix_val = row.get('prefix', '')
        if pd.isna(prefix_val):
            prefix_val = ''
        else:
            prefix_val = str(prefix_val).strip()

        samed_val = row.get('samed', '')
        if pd.isna(samed_val):
            samed_val = ''
        else:
            samed_val = str(samed_val).strip()

        var_val = row.get('Variable', '')
        if pd.isna(var_val):
            var_val = ''
        else:
            var_val = str(var_val).strip()

        default_val = row.get('Default', '')
        if pd.isna(default_val):
            default_val = ''

        mappings.append({
            'type': str(row.get('Type', '')).strip().lower(),
            'var': var_val,
            'prefix': prefix_val,
            'path': str(row['Path']).strip(),
            'datatype': str(row['DataType']).strip().lower(),
            'samed': samed_val,
            'Default': str(default_val).strip(),
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

def convert_value_with_default(csv_val, dtype, mapping_default):
    if pd.isna(csv_val) or (isinstance(csv_val, str) and csv_val.strip() == ""):
        if mapping_default and mapping_default.strip() != "":
            try:
                if dtype == "number":
                    return float(mapping_default)
                elif dtype == "boolean":
                    return str(mapping_default).strip().lower() in ["true", "1", "yes"]
                elif dtype in ["string", "date"]:
                    return str(mapping_default)
            except:
                return get_default_value(dtype)
        else:
            return get_default_value(dtype)
    else:
        return convert_value(csv_val, dtype)

def insert_path_nested(d, path, key, value):
    keys = path.split('/')
    for k in keys[:-1]:
        d = d.setdefault(k, {})
    last_key = keys[-1]
    if last_key not in d:
        d[last_key] = {}
    d[last_key][key] = value

def insert_path_direct(d, path, value_dict):
    keys = path.split('/')
    for k in keys[:-1]:
        d = d.setdefault(k, {})
    d[keys[-1]] = value_dict

def process_row_with_default(row, mappings, all_headers):
    final = {}
    list_struct = defaultdict(lambda: defaultdict(dict))

    header_map = {col.lower(): col for col in all_headers}
    row_dict = {col.lower(): val for col, val in row.items()}

    list_field_max_index = defaultdict(int)

    list_vars_by_path = defaultdict(list)
    for m in mappings:
        if m['type'] == 'list':
            list_vars_by_path[m['path']].append(m)

    for m in mappings:
        mtype = m['type']
        var = m['var']
        prefix = m['prefix']
        path = m['path']
        dtype = m['datatype']
        mapping_default = m.get('Default', "")

        var_lc = var.lower()
        prefix_lc = prefix.lower() if prefix else ""

        if mtype == "list":
            # Flexible regex matching columns like prefix + optional space/_ + number
            pattern_primary = re.compile(rf"^{re.escape(prefix_lc)}\s*_?(\d+)", re.IGNORECASE)
            matched = False

            for col_lc in header_map:
                match = pattern_primary.match(col_lc)
                if match:
                    idx = int(match.group(1)) - 1
                    # Key = prefix+index if var blank, else prefix+var (all lowercase)
                    if var.strip() == "":
                        key_name = f"{prefix}{idx + 1}"
                    else:
                        key_name = f"{prefix}{var}".lower()

                    csv_val = row_dict.get(col_lc, None)
                    value = convert_value_with_default(csv_val, dtype, mapping_default)

                    key_name = key_name.lower()
                    list_struct[path][idx][key_name] = value
                    list_field_max_index[path] = max(list_field_max_index[path], idx + 1)
                    matched = True

            if not matched:
                if var.strip() == "":
                    key_name = f"{prefix}1"
                else:
                    key_name = f"{prefix}{var}".lower()
                value = convert_value_with_default(None, dtype, mapping_default)
                list_struct[path][0][key_name] = value
                list_field_max_index[path] = max(list_field_max_index[path], 1)

        else:
            if var_lc in row_dict:
                csv_val = row_dict[var_lc]
                value = convert_value_with_default(csv_val, dtype, mapping_default)
            else:
                value = convert_value_with_default(None, dtype, mapping_default)
            insert_path_nested(final, path, var, value)

    for path, max_index in list_field_max_index.items():
        vars_for_path = list_vars_by_path[path]
        items = []
        for i in range(max_index):
            item = list_struct[path].get(i, {})
            for mvar in vars_for_path:
                if mvar['var'].strip() == "":
                    vname = f"{mvar['prefix']}{i + 1}".lower()
                else:
                    vname = f"{mvar['prefix']}{mvar['var']}".lower()

                if vname not in item:
                    item[vname] = convert_value_with_default(None, mvar['datatype'], mvar.get('Default', ""))
            items.append(item)
        insert_path_direct(final, path, items)

    return final

def process_csv_to_json(mapping_file, csv_file, output_file):
    mappings = read_mapping(mapping_file)
    df = pd.read_csv(csv_file)
    all_headers = list(df.columns)

    print("=== DEBUG: CSV Headers ===")
    print(all_headers)

    result = {"Quote": []}
    for _, row in df.iterrows():
        result["Quote"].append(process_row_with_default(row, mappings, all_headers))

    with open(output_file, 'w') as f:
        json.dump(result, f, indent=4)
    print(f"✅ Output saved to {output_file}")

# === RUN ===
process_csv_to_json("working mapping.xlsx", "rrf.csv", "output.json")
