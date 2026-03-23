def hojin_number_db_check(hojin_no: str, kijun_ym: str, session) -> bool:

    sql = f"""
    SELECT COUNT(*) AS cnt
    FROM T456SMMMD010
    WHERE HOJIN_NO = '{hojin_no}'
      AND KIJUN_YM = '{kijun_ym}'
    """

    try:
        df = session.sql(sql).to_pandas()
        count = df.iloc[0]["cnt"]
        return count >= 1
    except Exception:
        return False
