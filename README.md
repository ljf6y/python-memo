from snowflake.snowpark.context import get_active_session
from sql_reader import SqlReader
from sql_runner import SqlRunner


def hojin_number_db_check(hojin_no: str, kijun_ym: str) -> bool:

    session = get_active_session()

    reader = SqlReader("@MAIN_H.APP.STREAMLIT_COMMON_STAGE/sql/sc_5_01_05_0.sql")
    runner = SqlRunner(session, reader)

    df = runner.query(
        "check_hojin_number",
        {
            "hojin_no": hojin_no,
            "kijun_ym": kijun_ym
        }
    )

    rows = df.collect()

    if len(rows) == 0:
        return False

    count = rows[0]["CNT"]
    return count >= 1
