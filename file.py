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

        # === DEFAULT FEATURE START ===
        # Read Default column (if you remove default feature, delete this block)
        default_val = row.get('Default', '')
        if pd.isna(default_val):
            default_val = ''
        # === DEFAULT FEATURE END ===

        mappings.append({
            'type': str(row.get('Type', '')).strip().lower(),
            'var': str(row['Variable']).strip(),
            'prefix': prefix_val,
            'path': str(row['Path']).strip(),
            'datatype': str(row['DataType']).strip().lower(),
            'samed': samed_val,
            # === DEFAULT FEATURE START ===
            # Store default in mapping (delete if removing default feature)
            'Default': str(default_val).strip(),
            # === DEFAULT FEATURE END ===
        })
    return mappings

def get_default_value(dtype):
    """Standard fallback if both CSV and Mapping default are missing"""
    return {
        "string": "",
        "date": "",
        "number": 0,
        "boolean": False
    }.get(dtype, None)

def convert_value(val, dtype):
    """Normal converter (when val exists)"""
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

# === DEFAULT FEATURE START ===
def convert_value_with_default(csv_val, dtype, mapping_default):
    """
    Default-aware converter:
      1. If CSV has a value → normal conversion
      2. If CSV missing/empty → try mapping_default
      3. If mapping_default empty → fallback to datatype default
    Remove this function if you remove default feature
    """
    if pd.isna(csv_val) or (isinstance(csv_val, str) and csv_val.strip() == ""):
        # CSV is empty → try mapping default
        if mapping_default and mapping_default.strip() != "":
            try:
                if dtype == "number":
                    return float(mapping_default)
                elif dtype == "boolean":
                    return str(mapping_default).strip().lower() in ["true", "1", "yes"]
                elif dtype in ["string", "date"]:
                    return str(mapping_default)
            except:
                # if mapping default cannot be converted, fallback
                return get_default_value(dtype)
        else:
            return get_default_value(dtype)
    else:
        # CSV has a value → normal conversion
        return convert_value(csv_val, dtype)
# === DEFAULT FEATURE END ===

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

    # Group list vars by path for filling missing keys
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
        samed = m['samed']
        # === DEFAULT FEATURE START ===
        mapping_default = m.get('Default', "")  # get default for this field
        # === DEFAULT FEATURE END ===

        var_lc = var.lower()
        prefix_lc = prefix.lower() if prefix else ""
        samed_lc = samed.lower() if samed else ""

        if mtype == "list":
            # Regex for numbered list columns like Pl_input1_b
            pattern_primary = re.compile(rf"{prefix_lc}(\d+)[_]?{var_lc}$")
            pattern_fallback = re.compile(rf"{samed_lc}(\d+)$") if samed_lc else None
            matched = False

            for col_lc in header_map:
                match = pattern_primary.fullmatch(col_lc)
                if not match and pattern_fallback:
                    match = pattern_fallback.fullmatch(col_lc)
                if match:
                    idx = int(match.group(1)) - 1
                    field_name = f"{prefix}{var}" if prefix else var
                    csv_val = row_dict.get(col_lc, None)
                    # === DEFAULT FEATURE START ===
                    # use default-aware converter
                    value = convert_value_with_default(csv_val, dtype, mapping_default)
                    # === DEFAULT FEATURE END ===
                    list_struct[path][idx][field_name] = value
                    list_field_max_index[path] = max(list_field_max_index[path], idx + 1)
                    matched = True

            # Handle single-column list variable without numeric suffix
            if not matched:
                single_col = f"{prefix}{var}" if prefix else var
                if single_col.lower() in row_dict:
                    csv_val = row_dict[single_col.lower()]
                    # === DEFAULT FEATURE START ===
                    value = convert_value_with_default(csv_val, dtype, mapping_default)
                    # === DEFAULT FEATURE END ===
                    list_struct[path][0][single_col] = value
                    list_field_max_index[path] = max(list_field_max_index[path], 1)
                    matched = True

            # If column not found at all, still keep structure with mapping default
            if not matched:
                fallback_name = f"{prefix}{var}" if prefix else var
                # === DEFAULT FEATURE START ===
                value = convert_value_with_default(None, dtype, mapping_default)
                # === DEFAULT FEATURE END ===
                list_struct[path][0][fallback_name] = value
                list_field_max_index[path] = max(list_field_max_index[path], 1)

        else:
            # Non-list fields → merge multiple under same path
            if var_lc in row_dict:
                csv_val = row_dict[var_lc]
                # === DEFAULT FEATURE START ===
                value = convert_value_with_default(csv_val, dtype, mapping_default)
                # === DEFAULT FEATURE END ===
            elif samed_lc and samed_lc in row_dict:
                csv_val = row_dict[samed_lc]
                # === DEFAULT FEATURE START ===
                value = convert_value_with_default(csv_val, dtype, mapping_default)
                # === DEFAULT FEATURE END ===
            else:
                # no CSV column → use mapping default
                # === DEFAULT FEATURE START ===
                value = convert_value_with_default(None, dtype, mapping_default)
                # === DEFAULT FEATURE END ===
            insert_path_nested(final, path, var, value)

    # Finalize list items → ensure all vars exist
    for path, max_index in list_field_max_index.items():
        vars_for_path = list_vars_by_path[path]
        items = []
        for i in range(max_index):
            item = list_struct[path].get(i, {})
            for mvar in vars_for_path:
                vname = f"{mvar['prefix']}{mvar['var']}" if mvar['prefix'] else mvar['var']
                if vname not in item:
                    # === DEFAULT FEATURE START ===
                    # fill missing list key with mapping default
                    item[vname] = convert_value_with_default(None, mvar['datatype'], mvar.get('Default', ""))
                    # === DEFAULT FEATURE END ===
            items.append(item)
        insert_path_direct(final, path, items)

    return final

def process_csv_to_json(mapping_file, csv_file, output_file):
    mappings = read_mapping(mapping_file)
    df = pd.read_csv(csv_file)
    all_headers = list(df.columns)

    result = {"Quote": []}
    for _, row in df.iterrows():
        result["Quote"].append(process_row_with_default(row, mappings, all_headers))

    with open(output_file, 'w') as f:
        json.dump(result, f, indent=4)
    print(f"✅ Output saved to {output_file}")

# === RUN ===
process_csv_to_json("working mapping.xlsx", "rrf.csv", "output.json")
