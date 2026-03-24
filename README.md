def hojin_number_db_check(hojin_no: str, kijun_ym: str) -> bool:

    df = runner.query(
        "check_hojin_number",
        {
            "hojin_no": hojin_no,
            "kijun_ym": kijun_ym
        }
    ).to_pandas()

    count = df.iloc[0]["CNT"]

    return count >= 1
