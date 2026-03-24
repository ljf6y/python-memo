from snowflake.snowpark.context import get_active_session
from sql_reader import SqlReader
from sql_runner import SqlRunner


def hojin_number_db_check(hojin_no: str, kijun_ym: str) -> bool:
    session = get_active_session()

    reader = SqlReader("@MAIN_H.APP.STREAMLIT_COMMON_STAGE/sql/sc_check.sql")

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


def validate_dataframe(df: pd.DataFrame, rules: list,) -> list:
    """
    DFのチェック処理
    df(pd.DataFrame)：チェック対象DF
    rules(list)：チェックルール
        例：
        validation_rules = {
            0: {"is_required": True, "kijunym_check": kijundate, "common_check": "yyyymmdd"},
            1: {"max_len": 6, "type": "alphanumeric"},
            2: {"max_len": 6, "type": "number", "db_check": "hojin_number"},
            3: {"is_required": True, "common_check": "number_17_0"},
            4: {"common_check": "number_20_3"}
        }
    Returns:
        エラーメッセージリスト
    """

    err_msg_list = []

    for idx, row in df.iterrows():
        for col_idx, rule in rules.items():
            col_name = df.columns[col_idx]
            val_str = str(row[col_name])

            # 必要チェック
            is_required_check = rule.get("is_required", None)
            # 桁チェック
            max_len = rule.get("max_len", None)
            # 文字種チェック
            col_type = rule.get("type", None)
            # リスト範囲チェック
            list_check = rule.get("list_check", None)
            # 共通チェック
            common_check = rule.get("common_check", None)
            # DB存在チェック
            db_check = rule.get("db_check", None)
            # 基準日チェック
            kijundate_check = rule.get("kijundate_check", None)

            # 必要チェック
            if is_required_check and is_required(val_str) == False:
                msg = message_util.get_messages_data("456ERR0001", col_name)
                err_msg_list.append(f"レコード {idx+1}: {msg}")

            # 桁チェック
            if max_len is not None and is_under_max_length(val_str, max_len) == False:
                msg = message_util.get_messages_data("456ERR4026", col_name, max_len)
                err_msg_list.append(f"レコード {idx+1}: {msg}")

            # 型チェック
            if col_type is not None and len(val_str):
                if col_type == "number" and is_number(val_str) == False:
                    msg = message_util.get_messages_data("456ERR4025", col_name, "半角数字")
                    err_msg_list.append(f"レコード {idx+1}: {msg}")

                # 【英数字のみ】と【英字のみ】チェックは半角スペースを許容する。
                elif col_type == "alphabet" and is_alphabet(val_str) == False:
                    msg = message_util.get_messages_data("456ERR4025", col_name, "半角英数字")
                    err_msg_list.append(f"レコード {idx+1}: {msg}")

                elif col_type == "alphanumeric" and is_alphanumeric(val_str) == False:
                    msg = message_util.get_messages_data("456ERR4025", col_name, "半角英数字")
                    err_msg_list.append(f"レコード {idx+1}: {msg}")

            # 共通チェック
            if common_check is not None and len(val_str):
                # 日付
                if common_check == "yyyymmdd":
                    result, rtn_message = is_yyyymmdd(val_str, col_name)
                    if result == False:
                        err_msg_list.append(f"レコード {idx+1}: {rtn_message}")

                elif common_check == "yyyymm":
                    result, rtn_message = is_yyyymm(val_str, col_name)
                    if result == False:
                        err_msg_list.append(f"レコード {idx+1}: {rtn_message}")

                elif common_check == "yyyy":
                    result, rtn_message = is_yyyy(val_str, col_name)
                    if result == False:
                        err_msg_list.append(f"レコード {idx+1}: {rtn_message}")

                else:
                    # 金額、数量、桁チェック
                    number_check_list = common_check.split("_")
                    if number_check_list[0] == "number":
                        result, rtn_message = is_currency(
                            val_str,
                            col_name,
                            int(number_check_list[1]),
                            int(number_check_list[2])
                        )
                        if result == False:
                            err_msg_list.append(f"レコード {idx+1}: {rtn_message}")

            # リスト範囲チェック
            if list_check is not None:
                chk_list = list_check
                if is_list_range(val_str, chk_list) == False:
                    msg = message_util.get_messages_data("456ERR4088", col_name)
                    err_msg_list.append(f"レコード {idx+1}: {msg}")

            # DB存在チェック
            if db_check is not None:
                # 法人番号DB存在チェック
                if db_check == "hojin_number":
                    kijun_ym = row["年月"]
                    result = hojin_number_db_check(val_str, kijun_ym)
                    if result == False:
                        param = f"{col_name}:{val_str}"
                        msg = message_util.get_messages_data("456ERR0002", param)
                        err_msg_list.append(f"レコード {idx+1}: {msg}")

            # ファイル名に設定した基準年月との相関チェック
            if kijundate_check is not None and kijundate_check != val_str[:6]:
                msg = message_util.get_messages_data("456ERR4066", col_name)
                err_msg_list.append(f"レコード {idx+1}: {msg}")

    return err_msg_list


sc_check.sql

-- name: check_hojin_number
SELECT COUNT(*) AS CNT
FROM {{db}}.{{schema}}.T456SMMD010
WHERE HOJIN_NO = :hojin_no
  AND KIJUN_YM = :kijun_ym
;





