
def hojin_number_db_check(hojin_no: str, ym: str, session) -> bool:
    """
    法人番号DBチェック
    法人番号 + 年月 でDB存在チェックを行う
    """

    sql = f"""
    SELECT COUNT(*) AS cnt
    FROM T456040
    WHERE HOJIN_NO = '{hojin_no}'
      AND YM = '{ym}'
    """

    try:
        df = session.sql(sql).to_pandas()

        # 取件数
        count = df.iloc[0]["cnt"]

        # 判定：1件以上ならOK
        return count >= 1

    except Exception as e:
        # TODO: ログ出力（必要に応じて）
        return False