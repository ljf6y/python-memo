const_fmt005_total_col = 9

const_total_col



exp_col_list = dict(list(column_map.items())[:const_total_col])

if len(upload_df_after.columns) != const_total_col:
    err_msg_list.append(
        message_util.get_messages_data("456ERR4041", const_total_col)
    )


# 外部
import streamlit as st
import pandas as pd

# 内部
import checks
import message_util
