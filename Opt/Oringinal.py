import json
import xlwings as xw
import pandas as pd
from datetime import datetime
import re
from tqdm import tqdm
from math import prod

# --- Paths (adjust these) ---
rater_path = "RRF_Combined XoL template v17.xlsm"
template_path = "RRF_TestCase_Scenario_Batches.xlsx"
data_path = "new_target_V10_with_Underwriters.json" #user input
password = "RSP"

# ---------------- Your existing helpers ----------------
def flatten_json(obj, parent_key='', sep='.'):
    items = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            items.extend(flatten_json(v, new_key, sep=sep).items())
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            new_key = f"{parent_key}[{i}]"
            items.extend(flatten_json(v, new_key, sep=sep).items())
    else:
        items.append((parent_key, obj))
    return dict(items)

def fill_cell_or_range(ws, cell_ref, value):
    """Fill single cell or range depending on mapping type."""
    if ":" in cell_ref and"," in cell_ref:
        ranges = cell_ref.split(",")
        for r in ranges:
            rng = ws.range(r)
            cells = rng.rows.count
            if isinstance(value, list):
                for i in range(cells):
                    rng[i].value = value[i] if i < len(value) else None
            else:
                rng[0].value = value
                for i in range(1, cells):
                    rng[i].value = None
    elif ":" in cell_ref:       
# Special case: reverse fill for D94:D103 or F94:F103
        if cell_ref =="F94:F103":        
            rng = ws.range(cell_ref)
            cells = rng.rows.count

            if isinstance(value, list):
                # Fill from bottom (reverse)
                for i in range(cells):
                    rng[cells - 1 - i].value = value[i] if i < len(value) else None
            else:
                # If single value, fill bottom cell only
                rng[-1].value = value
                for i in range(cells - 1):
                    rng[i].value = None

        else:
            rng = ws.range(cell_ref)  # Range like B5:B15
            cells = rng.rows.count
            if isinstance(value, list):
                for i in range(cells):
                    rng[i].value = value[i] if i < len(value) else None
            else:
                rng[0].value = value
                for i in range(1, cells):
                    rng[i].value = None
    elif "!" in cell_ref:
        rng_injury = ws.range("D30:D44")
        rng_pd = ws.range("D48:D62")
        rng_poll = ws.range("D66:D80")
        cells = rng_injury.rows.count
        if isinstance(value, list):
            for i in range(cells):
                rng_injury[i].value = value[i] if i < len(value) else None
                rng_pd[i].value = value[i] if i < len(value) else None
                rng_poll[i].value = value[i] if i < len(value) else None
        else:
            rng[0].value = value
            for i in range(1, cells):
                rng[i].value = None 
    else:
        ws.range(cell_ref).value = value

def extract_matching_values(flat_data, pattern):
    parts = [part.strip() for part in pattern.split('.') if part.strip()]
    word_count = [len(part.split()) for part in parts]
    total_words = sum(word_count)
    if total_words == 5:  
        
        regex = re.compile(
            r'^'
            + re.escape(parts[0]) + r'\.'           # segment 1 + '.'
            + re.escape(parts[1]) + r'\[\d+\]\.'    # segment 2 + [digits] + '.'
            + re.escape(parts[2]) + r'\.'           # segment 3 + '.'
            r'([^.\s]+)\.'                          # segment 4: any non-dot, non-space token + '.'
            + re.escape(parts[-1])                  # segment 5: literal from pattern
            + r'$'
        )

    elif total_words == 2:
        regex = re.compile(re.escape(pattern.split('.')[0]) + r"\[\d+\]\." + re.escape(pattern.split('.')[-1]) + r"$")
    else:
        regex = re.compile(re.escape(pattern.split('.')[0]) + "." + re.escape(pattern.split('.')[1]) + r"\[\d+\]\." + re.escape(pattern.split('.')[-1]) + r"$")
    matches = [v for k, v in flat_data.items() if regex.search(k)]
    return matches

def transform_value(json_key, value):
    """
    Apply business mapping rules for specific fields.
    """
    if not isinstance(value, str):
        return value

    # Portfolio mapping
    if "portfolio" in json_key.lower():
        mapping = {
            "RM-EU": "RM Europe"
        }
        return mapping.get(value, value)

    # Producing Office mapping
    if "producingoffice" in json_key.lower():
        mapping = {
            "NL": "Netherlands",
            "ES": "Spain",
            "FR": "France",
            "BE": "Belgium"
        }
        return mapping.get(value, value)

    return value



def fill_json_to_excel(wb, mapping, flat_data):
    trade_cell_ref = []
    value_list = []
    summed_tech = 0
    for _, row in mapping.iterrows():
        sheet_name = row['Tab Ref']
        json_key = row['json_key']
        cell_ref = row['Cell Ref']
        if str(cell_ref) == "0":
            continue
        ws = wb.sheets[sheet_name]

        protect = ws.api.ProtectContents
        if protect:
            try:
                ws.api.Unprotect(Password=password)
            except:
                print("Could not unprotect")

        try:
            if ":" in cell_ref and"," in cell_ref:
                cells = cell_ref.split(",")
                for i in range(0, len(cells)):
                    if i == 1:
                        json_key_temp = json_key.split(".")[0] + "." + json_key.split(".")[1] + "[" + str(5) + "]." + json_key.split(".")[-1]
                        fill_cell_or_range(ws, cells[i], flat_data[json_key_temp])
                    else:
                        values = extract_matching_values(flat_data, json_key)
                        if values:
                            values = [transform_value(json_key, v) for v in values]
                            fill_cell_or_range(ws, cell_ref, values)
                        else:
                            fill_cell_or_range(ws, cell_ref, [])
            elif ":" in cell_ref:  # Range mapping
                values = extract_matching_values(flat_data, json_key)
                if values:
                    values = [transform_value(json_key, v) for v in values]
                    fill_cell_or_range(ws, cell_ref, values)
                else:
                    fill_cell_or_range(ws, cell_ref, [])
            elif "," in cell_ref:
                cells = cell_ref.split(",")
                for i in range(0, len(cells)):
                    json_key_temp = json_key.split(".")[0] + "." + json_key.split(".")[1] + "[" + str(i) + "]." + json_key.split(".")[-1]
                    fill_cell_or_range(ws, cells[i], flat_data[json_key_temp])
            elif "!" in cell_ref:
                values = extract_matching_values(flat_data,json_key)
                chunks = [values[i:i+5] for i in range(0, len(values), 5)]
                result = [((1 + x) for x in chunk)-1 for chunk in chunks] #in case of any change in tech adj formula, update here
                fill_cell_or_range(ws, cell_ref,result)
            else:
                val = flat_data[json_key]
                val = transform_value(json_key, val)
                fill_cell_or_range(ws, cell_ref, val)

        except:
            continue

# ---------------- NEW: Helpers to place covers & deductibles dynamically ----------------
def _build_label_row_map(ws, label_range: str):
    """
    Build a mapping from label text to absolute row number for a vertical range.
    Returns (label_to_row, first_empty_row).
    """
    rng = ws.range(label_range)  # e.g., 'D114:D135' or 'D37:D45'
    vals = rng.value

    # Normalize to list
    if isinstance(vals, list):
        flat = [v[0] if isinstance(v, list) else v for v in vals]
    else:
        flat = [vals]

    start_row = rng.row
    label_to_row = {}
    first_empty_row = None

    for i, v in enumerate(flat):
        row = start_row + i        
        if v is None:
            v_str = ""
        elif isinstance(v, float) and v.is_integer():
            v_str = str(int(v))  # Convert 3.0 → "3"
        else:
            v_str = str(v).strip()

        if v_str:
            label_to_row[v_str] = row
        elif first_empty_row is None:
            first_empty_row = row

    return label_to_row, first_empty_row

def _ensure_label_row(ws, label_value: str, label_col_letter: str, label_range: str):
    """
    Ensure the label exists within the label_range; if missing, place it in the first empty row.
    Returns the row number or None if no space left.
    """
    label_value = (label_value or "").strip()
    if not label_value:
        return None

    label_map, first_empty = _build_label_row_map(ws, label_range)
    if label_value in label_map:
        return label_map[label_value]

    if not first_empty:
        print(f"[WARN] No empty row left to place '{label_value}' in {label_range}")
        return None

    ws.range(f"{label_col_letter}{first_empty}").value = label_value
    return first_empty

def write_cover(ws, cover_obj: dict):
    """
    Places a cover by its 'cover' name in D114:D135 and writes attach/limit/aggregate
    into F/G/H on the same row.
    """
    cover_name = (cover_obj or {}).get('cover')
    if not cover_name:
        return

    row = _ensure_label_row(ws,
                            label_value=cover_name,
                            label_col_letter="D",
                            label_range="D114:D135")
    
    if not row:
        return

    # Write the available fields
    ws.range(f"E{row}").value = "Y"
    if 'attach' in cover_obj:
        ws.range(f"F{row}").value = cover_obj.get('attach')
    if 'limit' in cover_obj:
        ws.range(f"G{row}").value = cover_obj.get('limit')
    if 'aggregate' in cover_obj:
        ws.range(f"H{row}").value = cover_obj.get('aggregate')

def write_deductible(ws, ded_obj: dict):
    """
    Places a deductible by its 'code' in D37:D45 and writes description/F/G amounts
    into E/F/G on the same row.
    """
    code = (ded_obj or {}).get('code')
    if not code:
        return
    

    row = _ensure_label_row(ws,
                            label_value=code,
                            label_col_letter="D",
                            label_range="D37:D45")    
    if not row:
        return

    # Write the available fields
    description = ded_obj.get('description')

    if description:
        if description == "Percentage Deductible":
            ws.range(f"E{row}").value = ""
        else:
            ws.range(f"E{row}").value = description
    if 'nonRankingAmt' in ded_obj:
        ws.range(f"F{row}").value = ded_obj.get('nonRankingAmt')
    if 'rankingAmt' in ded_obj:
        ws.range(f"G{row}").value = ded_obj.get('rankingAmt')




def write_percentage_deductibles(ws, deductibles):
    """
    Populate PL Information % section:
    - F47:F49 → NonRankingPct
    - I47:I49 → Min
    - J47:J49 → Max

    Only for amtOrPct = "P"
    Fill sequentially top-down
    """

    start_row = 47

    # ✅ Step 0: Clear existing values
    ws.range("F47:F49").value = [[None], [None], [None]]
    ws.range("I47:I49").value = [[None], [None], [None]]
    ws.range("J47:J49").value = [[None], [None], [None]]

    # ✅ Step 1: Filter only percentage deductibles
    p_deds = [
        d for d in deductibles
        if str(d.get("amtOrPct", "")).upper() == "P"
    ]

    # ✅ Step 2: Fill sequentially
    for i, d in enumerate(p_deds):
        if i >= 3:
            break

        row = start_row + i

        non_pct = d.get("nonRankingPct") or 0
        min_val = d.get("min") or 0
        max_val = d.get("max") or 0

        ws.range(f"F{row}").value = non_pct
        ws.range(f"I{row}").value = min_val
        ws.range(f"J{row}").value = max_val

def write_UW(ws, UW_obj: dict):
    """
    Places a deductible by its 'code' in D37:D45 and writes description/F/G amounts
    into E/F/G on the same row.
    """
    range = (UW_obj or {}).get('range')
    item = (UW_obj or {}).get('item')
    limit = (UW_obj or {}).get('limit')
    expense = (UW_obj or {}).get('expense')
    cover = (UW_obj or {}).get('cover')

    range_UW = next((x for x in [range, limit, item, expense, cover] if x), None)
    if not range_UW:
        return
    
    row = _ensure_label_row(ws,
                            label_value=range_UW,
                            label_col_letter="C",
                            label_range="C35:C152")    
    if not row:
        return
    # Write the available fields
    if 'underwriterView' in UW_obj:
        ws.range(f"G{row}").value = UW_obj.get('underwriterView')
    if 'premiumBasis' in UW_obj:
        ws.range(f"D{row}").value = UW_obj.get('premiumBasis')
# ---------------- Main ----------------
df = pd.read_excel(template_path, sheet_name="Main")
mapping_df = df[df["Mandatory"] == "Y"]

BP_before_deductibles = []
TP_before_deductible = []
BP_after_deductibles = []
TP_after_deductible = []
BP_incl_COC = []
TP_incl_COC = []
BP_GOC = []
TP_GOC = []
quoteID = []


def write_claim_deductible_indicator(ws, claims, ded_code_type_map):
    """
    Writes deductibleIndicator explicitly to Excel
    Applies PC logic ONLY here
    """

    start_row = 11   # ⚠️ adjust if your claim rows start elsewhere
    col = "AM"        # ⚠️ adjust to your actual deductibleIndicator column

    for i, cl in enumerate(claims):
        row = start_row + i

        indicator = str(cl.get("deductibleIndicator", "")).strip()

        if indicator in ded_code_type_map:
            if ded_code_type_map[indicator] == "P" and indicator.isdigit():
                final_val = f"PC{indicator}"
            else:
                final_val = indicator
        else:
            final_val = indicator

        ws.range(f"{col}{row}").value = final_val


with open(data_path, 'r') as f:
    raw_data = json.load(f)
app = xw.App(visible=True)
app.display_alerts = False
app.ask_to_update_links = False
for quote in tqdm(raw_data['quote'], desc="Processing quotes", unit="quote"):
            
    # if quote['quoteId'] in ["RRFTC_1001_claims"]:

    # try:
        # Flatten for your mapping-driven fills
        wb = app.books.open(rater_path, update_links=False)
        app.api.EnableEvents = False
        app.api.ScreenUpdating = False
        app.api.DisplayAlerts = False
        app.api.Calculation = -4135  # Manual
        data = flatten_json(quote)
        fill_json_to_excel(wb, mapping_df, data)

        

        # # --- NEW: Dynamic placement by cover / deductible codes ---
        ws_info = wb.sheets['PL Information']
        ws_UW = wb.sheets['PL Component Price']
        # Unprotect temporarily (if needed)
        was_protected = False
        try:
            was_protected = bool(ws_info.api.ProtectContents)
            ws_info.api.Unprotect(Password=password)
            ws_UW.api.Unprotect(Password=password)
        except Exception as e:
            print(f"[WARN] Could not unprotect: {e}")

        # Covers (support either quote['plCoverInfo']['covers'] or quote['covers'])
        covers = ((quote.get('plCoverInfo', {}) or {}).get('covers', [])) or quote.get('covers', []) or []
        
        for cov in covers:
            write_cover(ws_info, cov)

        # Deductibles (support either quote['plCoverInfo']['deductibles'] or quote['deductibles'])
        deductibles = ((quote.get('plCoverInfo', {}) or {}).get('deductibles', [])) or quote.get('deductibles', []) or []
        
        ded_code_type_map = {}

        
        for d in deductibles:
            code = str(d.get("code")).strip()
            amt_type = str(d.get("amtOrPct")).upper()
            ded_code_type_map[code] = amt_type
            write_deductible(ws_info, d)
            write_percentage_deductibles(ws_info, deductibles)
        claims_list = quote.get("claims", []) or []
        ws_claims = wb.sheets['PL Claims List']  

        write_claim_deductible_indicator(ws_claims, claims_list, ded_code_type_map)

        #for ded in deductibles:

        # -------- NEW: Claims Deductible Indicator Logic (Excel-driven PC) --------
        try:
            ws_claims = wb.sheets['PL Claims List']

            was_protected_claims = False
            try:
                was_protected_claims = bool(ws_claims.api.ProtectContents)
                ws_claims.api.Unprotect(Password=password)
            except:
                pass

            if was_protected_claims:
                ws_claims.api.Protect(Password=password, DrawingObjects=True, Contents=True, Scenarios=True)

        except Exception as e:
            print(f"[WARN] deductibleIndicator update failed: {e}")



        columns_UW = ['claimsTo5m','largeClaimsTo1m','otherExpenses','pricingForLimits','usaDomiciledExposure','extensions']
        
        pl_info = quote.get('plCoverInfo') or {}
        covers_list = pl_info.get('covers') or []  # <-- NOTE: 'covers' (plural)
        pl_covers = set()
        
        for c in covers_list:
            if isinstance(c, dict):
                name = c.get('cover')
            else:
                name = c
            if name is not None:
                pl_covers.add(str(name).strip().lower())
        # print(pl_covers)
        for col in columns_UW:
            UW_view = ((quote.get('componentPrice', {}) or {}).get(col, []))
            for values in UW_view:
                if col == "extensions":
                    if not isinstance(values, dict):
                        continue
                    cover_val = str(values.get("cover", "")).strip().lower()
                    if cover_val in pl_covers:
                        uw_view_val = values.get("underwriterView")
                        write_UW(ws_UW, values)
                else:
                    write_UW(ws_UW, values)

                     
            

        
        exposure_len = quote.get('expRating',{}).get('exposures',[])
        for i in range(0,len(exposure_len)):
            ws_info.range(f"J{30-i}").value =  "Y"

        # Re-protect if it was protected earlier
        try:
            if was_protected:
                ws_info.api.Protect(Password=password, DrawingObjects=True, Contents=True, Scenarios=True)
        except Exception as e:
            print(f"[WARN] Could not re-protect PL Information: {e}")

        # Calculate workbook
        app.api.Calculate()

        # Read outputs
        sheet = wb.sheets['PL Book Rating']
        sheet2 = wb.sheets['PL Component Price']

        # BP_before_deductibles.append(sheet.range('C140').value)
        # TP_before_deductible.append(sheet.range('D140').value)
        # BP_after_deductibles.append(sheet.range('C168').value)
        # TP_after_deductible.append(sheet.range('D168').value)
        # BP_incl_COC.append(sheet.range('H140').value)
        # TP_incl_COC.append(sheet.range('I140').value)
        BP_GOC.append(sheet2.range('J178').value)
        TP_GOC.append(sheet2.range('K178').value)
        quoteID.append(wb.sheets['Front Page'].range('E9').value)

        # Restore Excel settings
        app.api.Calculation = -4105
        app.api.ScreenUpdating = True
        qid = wb.sheets['Front Page'].range('E9').value
        

        # wb.save(f"Rater_{qid}.xlsm")   
        wb.close()
    #except:
        # print(Exception)
        # premium_dict = {
        #     'quoteID': quoteID,
        #     # 'BP_before_deductibles': BP_before_deductibles,
        #     # 'TP_before_deductible': TP_before_deductible,
        #     # 'BP_after_deductibles': BP_after_deductibles,
        #     # 'TP_after_deductible': TP_after_deductible,
        #     # 'BP_incl_COC': BP_incl_COC,
        #     # 'TP_incl_COC': TP_incl_COC,
        #     'BP_GOC': BP_GOC,
        #     'TP_GOC': TP_GOC
        # }

        # premium_df = pd.DataFrame(premium_dict)
        # premium_df.to_csv('E2E Rater Premiums.csv', index=False)
app.quit()


premium_dict = {
    'quoteID': quoteID,
    # 'BP_before_deductibles': BP_before_deductibles,
    # 'TP_before_deductible': TP_before_deductible,
    # 'BP_after_deductibles': BP_after_deductibles,
    # 'TP_after_deductible': TP_after_deductible,
    # 'BP_incl_COC': BP_incl_COC,
    # 'TP_incl_COC': TP_incl_COC,
    'System Based Premium GOC': BP_GOC,
    'Risk Based Premium GOC ': TP_GOC
}
    
premium_df = pd.DataFrame(premium_dict)
premium_df.to_csv('UW_V10_Results_500.csv', index=False)

