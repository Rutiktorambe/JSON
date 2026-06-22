
    # Original logic writes J30, J29, J28... depending on exposure count.
    last_row = 30
    first_row = 30 - len(exposures) + 1
    ws.range(f"J{first_row}:J{last_row}").value = [["Y"] for _ in exposures]


def configure_excel_for_batch(app):
    app.display_alerts = False
    app.ask_to_update_links = False
    app.api.EnableEvents = False
    app.api.ScreenUpdating = False
    app.api.DisplayAlerts = False
    app.api.Calculation = -4135  # xlCalculationManual


def restore_excel(app):
    try:
        app.api.Calculation = -4105  # xlCalculationAutomatic
        app.api.ScreenUpdating = True
        app.api.EnableEvents = True
        app.api.DisplayAlerts = True
    except Exception:
        pass


def format_duration(seconds):
    seconds = int(round(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def main():
    start_time = time.perf_counter()

    df = pd.read_excel(template_path, sheet_name="Main")
    mapping_df = df[df["Mandatory"] == "Y"].rename(
        columns={"Tab Ref": "Tab_Ref", "Cell Ref": "Cell_Ref"}
    )
    mapping_rules = build_mapping_rules(mapping_df)

    with open(data_path, "r", encoding="utf-8") as file:
        raw_data = json.load(file)

    quotes = raw_data["quote"]
    quote_id = []
    bp_goc = []
    tp_goc = []

    app = xw.App(visible=False)
    configure_excel_for_batch(app)

    try:
        for quote in tqdm(quotes, desc="Processing quotes", unit="quote"):
            wb = None

            try:
                wb = app.books.open(str(Path(rater_path).resolve()), update_links=False)

                flat_data = flatten_json(quote)
                fill_json_to_excel(wb, mapping_rules, flat_data)

                ws_info = wb.sheets["PL Information"]
                ws_uw = wb.sheets["PL Component Price"]
                ws_claims = wb.sheets["PL Claims List"]

                dynamic_protection = unprotect_sheets_once(
                    wb,
                    ["PL Information", "PL Component Price", "PL Claims List"],
                )

                try:
                    covers = ((quote.get("plCoverInfo", {}) or {}).get("covers", [])) or quote.get("covers", []) or []
                    deductibles = ((quote.get("plCoverInfo", {}) or {}).get("deductibles", [])) or quote.get("deductibles", []) or []

                    write_covers(ws_info, covers)
                    ded_code_type_map = write_deductibles(ws_info, deductibles)
                    write_claim_deductible_indicator(ws_claims, quote.get("claims", []) or [], ded_code_type_map)
                    write_underwriter_views(ws_uw, quote)
                    write_exposure_flags(ws_info, quote)
                finally:
                    reprotect_sheets(wb, dynamic_protection)

                app.api.Calculate()

                sheet2 = wb.sheets["PL Component Price"]
                front_page = wb.sheets["Front Page"]

                bp_goc.append(sheet2.range("J178").value)
                tp_goc.append(sheet2.range("K178").value)
                quote_id.append(front_page.range("E9").value)

            finally:
                if wb is not None:
                    wb.close(SaveChanges=False)

    finally:
        restore_excel(app)
        app.quit()

    premium_df = pd.DataFrame(
        {
            "quoteID": quote_id,
            "System Based Premium GOC": bp_goc,
            "Risk Based Premium GOC ": tp_goc,
        }
    )
    premium_df.to_csv(output_path, index=False)

    elapsed_seconds = time.perf_counter() - start_time
    cases = len(quotes)
    projected_1000_seconds = elapsed_seconds / max(cases, 1) * 1000
    saved_seconds = baseline_seconds_for_1000_cases - projected_1000_seconds

    print(f"Processed {cases} cases in {format_duration(elapsed_seconds)}")
    print(f"Projected time for 1000 cases: {format_duration(projected_1000_seconds)}")

    if saved_seconds > 0:
        print(f"Estimated reduction vs 5h baseline: {format_duration(saved_seconds)}")
    else:
        print(f"No reduction vs 5h baseline. Difference: {format_duration(abs(saved_seconds))} slower")


if __name__ == "__main__":
    main()
