# ★★★【修正：DBチェック呼び出し】★★★
if db_check is not None:
    if db_check == "hojin_number":

        kijun_ym = row["年月"]   # ← 这里你前面已经确认列名了

        result = hojin_number_db_check(val_str, kijun_ym, session)

        if result == False:
            param = f"{col_name}:{val_str}"
            msg = message_util.get_messages_data("456ERR0002", param)
            err_msg_list.append(f"レコード {idx+1}: {msg}")
