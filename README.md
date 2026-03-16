for idx, value in enumerate(upload_df_after.iloc[:, 3], start=1):

    value = str(value).strip()

    if value != "" and value not in fudosan_shueki_type:

        err_msg_list.append(
            message_util.get_messages_data(
                "456ERR4088", "収益種類"
            )
        )