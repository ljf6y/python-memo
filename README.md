# 不動産粗利

import pandas as pd
import checks as checks
import message_util

from snowflake.snowpark.context import get_active_session
from sql_reader import SqlReader  
from sql_runner import SqlRunner


# Snowpark セッション取得
session = get_active_session()

# SQLファイル読込
reader = SqlReader("pages/sc_5_01_05_0.sql")
runner = SqlRunner(session, reader)


# 定数定義
const_fmt004_total_col = 7


# 不動産粗利の収益種類
fudosan_shueki_type = [
    "証券化信託",
    "土地信託",
    "管理信託",
    "管理委任",
    "投資法人事務",
    "仲介",
    "鑑定",
    "コンサル"
]


# ==================================================
# ファイル選択後イベント
# ==================================================
def fmt_check(upload_df: pd.DataFrame, ym: str) -> tuple[list, pd.DataFrame]:

    err_msg_list = []
    upload_df_after = upload_df

    # 項目数チェック
    if len(upload_df_after.columns) != const_fmt004_total_col:

        err_msg_list.append(
            message_util.get_messages_data("456ERR4041", const_fmt004_total_col)
        )

        return err_msg_list, upload_df_after

    else:

        # データクレンジング
        upload_df_after = clean_data(upload_df_after)

        # チェックルール設定
        validation_rules = {

            # 年月
            0: {"is_required": True, "common_check": "yyyymm"},

            # 法人番号
            1: {"max_len": 6, "type": "numeric", "db_check": "hojin_number"}, 

            # 取引先名
            2: {"max_len": 100},

            # 収益種類（共通チェックではなく個別チェック）
            3: {"is_required": True, "max_len": 50},

            # 収益種類補足
            4: {"max_len": 100},

            # 粗利金額
            5: {"is_required": True, "common_check": "number_17_0"},

            # 備考
            6: {"max_len": 100},
        }

        # 共通チェック
        err_msg_list = checks.validate_dataframe(
            upload_df_after,
            list(validation_rules.items())
        )


        for idx, value in enumerate(upload_df_after.iloc[:, 3], start=1):

            if pd.notna(value) and value != "":

                if value not in fudosan_shueki_type:

                    err_msg_list.append(
                        message_util.get_messages_data(
                            "456ERR4088", "収益種類"
                        )
                    )

        return err_msg_list, upload_df_after


# ==================================================
# データクレンジング
# ==================================================
def clean_data(upload_df: pd.DataFrame) -> pd.DataFrame:

    trim_cols = [
        "年月",
        "法人番号",
        "取引先名",
        "収益種類",
        "収益種類補足",
        "粗利金額",
        "備考"
    ]

    # 前後空白削除（半角＋全角）
    upload_df[trim_cols] = upload_df[trim_cols].apply(
        lambda col: col.apply(
            lambda v: v.replace("　", "").strip() if isinstance(v, str) else v
        )
    )

    return upload_df


# ==================================================
# 実行ボタン押下イベント
# ==================================================
def insert_to_work_table(
    upload_df: pd.DataFrame,
    uchiwake_code: str,
    ym: str,
    torikomi_id: str
) -> bool:

    # 既存データ件数取得
    result_list = runner.query(
        "get_t456uwoa040",
        {
            "uchiwake_code": uchiwake_code,
            "ym": ym
        }
    )

    if len(result_list) == 1 and result_list[0]["DATA_COUNT"] > 0:

        runner.execute(
            "del_t456uwoa040",
            {
                "uchiwake_code": uchiwake_code,
                "ym": ym
            }
        )

    # カラム名変換
    column_map = {
        "年月": "YM",
        "法人番号": "HOJIN_NO",
        "取引先名": "TORIHIKISAKI_MEI",
        "収益種類": "SHUEKI_TYPE",
        "収益種類補足": "SHUEKI_TYPE_SUB",
        "粗利金額": "ARARI_GAKU",
        "備考": "BIKOU"
    }

    upload_df = upload_df.rename(columns=column_map)

    # 空文字 → None
    upload_df = upload_df.replace("", None)

    # 内訳コード
    upload_df["UCHIWAKE_CD"] = uchiwake_code

    # ファイル内連番
    upload_df["FILENAME_RENBAN"] = upload_df.index + 1

    # 取込ID
    upload_df["TORIKOMI_ID"] = torikomi_id

    # Snowflake登録
    snowpark_df = session.write_pandas(
        upload_df,
        table_name="T456UWOA040",
        schema="UPLOAD",
        auto_create_table=False,
        overwrite=False
    )

    # 登録結果確認
    if len(snowpark_df.to_pandas()) != len(upload_df):
        return False
    else:
        return True
