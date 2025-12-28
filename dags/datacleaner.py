def data_cleaner(
    input_path: str = "/opt/airflow/store_files/raw_store_transactions.csv",
    output_path: str = "/opt/airflow/store_files/clean_store_transactions.csv",
):
    import pandas as pd
    import re
    from pathlib import Path

    input_path = str(Path(input_path).expanduser().resolve())
    output_path = str(Path(output_path).expanduser().resolve())

    df = pd.read_csv(input_path)
    rows_in = int(len(df))
    cols_in = list(df.columns)

    def clean_store_location(st_loc):
        if pd.isna(st_loc):
            return None
        return re.sub(r"[^\w\s]", "", str(st_loc)).strip()

    def clean_product_id(pd_id):
        if pd.isna(pd_id):
            return None
        matches = re.findall(r"\d+", str(pd_id))
        return matches[0] if matches else str(pd_id)

    def remove_dollar(amount):
        if pd.isna(amount):
            return None
        s = str(amount).strip().replace("$", "").replace(",", "")
        return float(s) if s else None

    # --- Track changes meaningfully ---
    change_summary = {}

    # STORE_LOCATION
    if "STORE_LOCATION" in df.columns:
        before = df["STORE_LOCATION"].astype("string")
        after = before.map(clean_store_location).astype("string")
        df["STORE_LOCATION"] = after
        change_summary["store_location_changed_rows"] = int(
            (before != after).fillna(False).sum()
        )

    # PRODUCT_ID
    if "PRODUCT_ID" in df.columns:
        before = df["PRODUCT_ID"].astype("string")
        after = before.map(clean_product_id).astype("string")
        df["PRODUCT_ID"] = after
        change_summary["product_id_changed_rows"] = int(
            (before != after).fillna(False).sum()
        )

    # Currency columns
    currency_cols = [c for c in ["MRP", "CP", "DISCOUNT", "SP"] if c in df.columns] # noqa: E501
    currency_converted = {}
    for col in currency_cols:
        before = df[col].astype("string")
        df[col] = df[col].map(remove_dollar)
        currency_converted[col] = {
            "non_null_before": int(before.notna().sum()),
            "non_null_after": int(df[col].notna().sum()),
        }
    if currency_cols:
        change_summary["currency_converted"] = currency_converted

    # Date column normalization to match table
    if "Date" in df.columns and "transaction_date" not in df.columns:
        df = df.rename(columns={"Date": "transaction_date"})
        change_summary["renamed_columns"] = {"Date": "transaction_date"}

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    rows_out = int(len(df))
    cols_out = list(df.columns)
    null_counts = {c: int(df[c].isna().sum()) for c in df.columns}

    # Return value -> XCom ("return_value")
    return {
        "input_path": input_path,
        "output_path": output_path,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "columns_in": cols_in,
        "columns_out": cols_out,
        "null_counts": null_counts,
        "changes": change_summary,
    }
