import pandas as pd
import json

# Path to your Excel file
file_path = "your_file.xlsx"

# Read all sheets
sheets = pd.read_excel(file_path, sheet_name=None)

final_json = {}

for sheet_name, df in sheets.items():
    # Normalize column names
    df.columns = df.columns.str.strip().str.lower()

    # Replace blanks or NaN with 1 for Parent, ParentID, instnaceID
    for col in ["parent", "parentid", "instnaceid"]:
        if col in df.columns:
            df[col] = df[col].replace("", 1).fillna(1).astype(int)

    for _, row in df.iterrows():
        risk_id = str(row['riskid'])
        parent_id = str(row['parentid'])
        inst_id = str(row['instnaceid'])

        # Create nested structure
        final_json.setdefault(risk_id, {})
        final_json[risk_id].setdefault(parent_id, {})
        final_json[risk_id][parent_id].setdefault(inst_id, [])

        # ---- KEY PART ----
        # Split variables into groups of 4 (a-d, e-h, i-l, etc.)
        var_cols = [col for col in df.columns if col not in ['parent', 'riskid', 'parentid', 'instnaceid']]
        var_cols.sort()  # ensures order: a,b,c,d,e,f,g,h...

        for i in range(0, len(var_cols), 4):  # take 4 at a time
            group = var_cols[i:i+4]
            group_dict = {col: row[col] for col in group if col in row}
            if group_dict:  # only add non-empty
                final_json[risk_id][parent_id][inst_id].append(group_dict)

# Save JSON
with open("output.json", "w") as f:
    json.dump(final_json, f, indent=4)

print("✅ JSON file created: output.json")
