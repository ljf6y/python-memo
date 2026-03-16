# 不動産粗利

import pandas as pd
import common.checks as checks
import message_util

from snowflake.snowpark.context import get_active_session
from sql_reader import sql_reader
from sql_runner import sql_runner


# Snowpark セッション取得
session = get_active_session()

# SQLファイル読込
reader = sql_reader("pages/sc_5_01_05_0.sql")
runner = sql_runner(session, reader)


# 定数定義
const_fmt004_total_col = 7


# 不動産粗利の収益種類
fudosan_shueki_type = {
    "証券化信託",
    "土地信託",
    "管理信託",
    "管理委任",
    "投資法人事務",
    "仲介",
    "鑑定",
    "コンサル"
}


# ==================================================
# ファイル選択後イベント
# ==================================================
def fmt_check(upload_df: pd.DataFrame, ym: str) -> tuple[list, pd.DataFrame]:
    """
    不動産粗利（汎用）_ファイル選択後イベント

    upload_df(pandas.DataFrame): アップロード情報
    ym(str): 基準年月（例：202603）

    Returns:
        list: エラーメッセージリスト
        pandas.DataFrame: 編集後アップロード情報
    """

    # 空きのエラーメッセージリスト、編集後アップロード情報を作成する
    err_msg_list = []
    upload_df_after = upload_df

    # アップロード情報の項目数チェック
    if len(upload_df_after.columns) != const_fmt004_total_col:

        err_msg_list.append(
            message_util.get_messages_data("456ERR4041", const_fmt004_total_col)
        )

        return err_msg_list, upload_df_after

    else:

        # データクレンジング処理
        upload_df_after = clean_data(upload_df_after)

        # チェックルール設定
        validation_rules = {

            # 年月
            0: {"is_required": True, "common_check": "yyyymm"},

            # 法人番号
            1: {"max_len": 6, "type": "number", "db_check": "hojin_number"},

            # 取引先名
            2: {"max_len": 100},

            # 収益種類
            3: {"is_required": True, "max_len": 50, "list_check": fudosan_shueki_type},

            # 収益種類補足
            4: {"max_len": 100},

            # 粗利金額
            5: {"is_required": True, "common_check": "number_17_0"},

            # 備考
            6: {"max_len": 100},
        }

        # 共通チェック部品（DFチェック）呼び出し
        err_msg_list = checks.validate_dataframe(
            upload_df_after,
            list(validation_rules.items())
        )

        # 処理終了
        return err_msg_list, upload_df_after


# ==================================================
# データクレンジング
# ==================================================
def clean_data(upload_df: pd.DataFrame) -> pd.DataFrame:
    """
    データクレンジング処理

    upload_df(pandas.DataFrame): アップロード情報

    Returns:
        pandas.DataFrame: 編集後アップロード情報
    """

    trim_cols = [
        "年月",
        "法人番号",
        "取引先名",
        "収益種類",
        "収益種類補足",
        "粗利金額",
        "備考"
    ]

    # 前後空白削除
    upload_df[trim_cols] = upload_df[trim_cols].apply(
        lambda x: x.str.strip()
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
    """
    不動産粗利（汎用）_実行ボタン押下イベント

    upload_df(pandas.DataFrame): アップロード情報
    uchiwake_code(str): 内訳コード
    ym(str): 基準年月
    torikomi_id(str): 取込ID

    Returns:
        bool: データ登録結果
    """

    # 既存データ件数取得
    result_list = runner.query(
        "get_t456uwoa040",
        {
            "uchiwake_code": uchiwake_code,
            "ym": ym
        }
    )

    # 既存データが存在する場合削除
    if len(result_list) == 1 and result_list[0] > 0:

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

    # 空文字をNoneに変換
    upload_df = upload_df.replace("", None)

    # 内訳コード追加
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
