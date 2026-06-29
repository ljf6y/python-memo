# データチェック
# 重複チェック
check_df = upload_df_after[["基準年月"]].copy()

has_dupes = check_df.duplicated().any()

if has_dupes:
    err_msg_list.append(
        message_util.get_messages_data(
            "456ERR4047",
            "基準年月"
        )
    )
    return err_msg_list, upload_df_after