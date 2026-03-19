
def hojin_number_db_check(hojin_no: str, ym: str, session) -> bool:

    sql = f"""
    SELECT COUNT(*) AS cnt
    FROM T456040
    WHERE HOJIN_NO = '{hojin_no}'
      AND kijun_ym = '{ym}'
    """

    try:
        df = session.sql(sql).to_pandas()

        count = df.iloc[0]["cnt"]

        return count >= 1

    except Exception as e:
        return False