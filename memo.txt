import streamlit as st
import pandas as pd
import checks as checks
import message_util as message_util
from snowflake.snowpark.context import get_active_session
from session_key import SESSION_KEY
from sql_reader import SqlReader
from sql_runner import SqlRunner

# Snowpark セッション取得
session = get_active_session()

# sqlファイルを読み込む
reader = SqlReader("pages/sql/sc_5_04_01_0.sql")
runner = SqlRunner(session, reader)

# 定数定義
const_total_col = 13
temban_index = "6"
yen_kansan_zumi_frg_index = "8"

# column_map定義
column_map = {
    "年月": "YM",
    "法人番号": "HOJIN_NO",
    "取引先名": "SAKI_MEI",
    "分類": "BUNRUI",
    "商品": "SHOHIN",
    "商品注": "SHOHIN_CHU",
    "店番": "TEMBAN",
    "属性": "ZOKUSEI",
    "円換算済フラグ": "YEN_KANSAN_ZUMI_FRG",
    "通貨": "TSUKA",
    "粗利金額（全体）": "ARARI_GAKU_ZENTAI",
    "粗利金額（事業）": "ARARI_GAKU_JIGYOU",
    "備考": "BIKOU",
    "内訳コード": "UCHIWAKE_CD",
    "ファイル内連番": "FILENAI_RENBAN",
    "取込ID": "TORIKOMI_ID"
}

# 分類設定内容リスト
shurui_list = [
    "外為",
    "デリ",
    "仕組",
    "外預",
    ""
]

# 辞書定義（分類と商品）
shurui_syohin_map = {
    "外為": [
        "為替売買益",
        "外為手数料",
        "外為保証料",
        "オプション料"
    ],
    "デリ": [
        "IRS",
        "CAP",
        "CCS",
        "FLR",
        "SWPN",
        "COLR",
        "CCAP",
        "RCAP",
        "IRSC",
        "BCAP",
        "特約付固定化",
        "仕組預金",
        "CAP付フローター",
        "特約付変動化",
        "RPI",
        "CPI"
    ],
    "仕組": [
        "仕組預金",
        "仕組借入",
        "仕組信託"
    ],
    "外預": [
        "カシヨ"
    ]
}

# 辞書定義（分類と商品注）
shurui_syohinchu_map = {
    "外為": [
        ""
    ],
    "デリ": [
        "一部解約",
        "全部解約",
        ""
    ],
    "仕組": [
        ""
    ],
    "外預": [
        ""
    ]
}

def fmt_check(upload_df: pd.DataFrame, ym: str) -> tuple[list, pd.DataFrame | None]:
    """
    マーケット対顧取引粗利_ファイル選択後イベント
    upload_df(pandas.DataFrame): アップロード情報
    ym(str): 基準年月（例：202603）
    Returns:
        list エラーメッセージリスト
        pandas.DataFrame 編集後アップロード情報
    """

    # エラーメッセージリスト、編集後アップロード情報を定義する
    err_msg_list = []
    upload_df_after = None

    # 対象フォーマットのファイル項目一覧の項目名を取得する
    exp_col_list = dict(list(column_map.items())[:const_total_col])

    # アップロード情報の項目数が13列外の場合
    if len(upload_df.columns) != const_total_col:
        err_msg_list.append(message_util.get_messages_data("456ERR4041", const_total_col))

        return err_msg_list, upload_df_after

    # 対象フォーマットのファイル項目一覧の項目名とアップロードされたファイルの項目名が一致しない場合
    elif checks.validate_dfheader(upload_df, exp_col_list) == False:
        err_msg_list.append(message_util.get_messages_data("456ERR4093"))

        return err_msg_list, upload_df_after

    else:

        # データクレンジング処理を行う
        upload_df_after = clean_data(upload_df)

        # 法人番号リストを取得する
        hojin_number_df = runner.query(
            "check_hojin_number_with_date",
            {
                "ym": ym
            }
        )

        # 想定外エラーの場合
        error_msg = st.session_state.get(SESSION_KEY.SS_KEY_DB_ERROR, None)
        if error_msg:
            return err_msg_list, upload_df_after

        # チェックルールを作成する
        validation_rules = [
            # 項目名：年月 → 必須チェック、基準年月チェック、共通チェック_日付（年月）
            {"is_required": True, "kijundate_check": ym, "common_check": "yyyymm"},
            # 項目名：法人番号 → 最大桁数チェック（6桁）、文字種チェック（半角数字）、DB存在チェック（法人番号）
            {"max_len": 6, "type": "numeric", "db_hojin_number_check": hojin_number_df.to_pandas()},
            # 項目名：取引先名 → 最大桁数チェック（100桁）
            {"max_len": 100},
            # 項目名：分類 → 必須チェック、最大桁数チェック（2桁）、個別チェック_分類設定内容チェック
            {"is_required": True, "max_len": 2, "list_check": shurui_list},
            # 項目名：商品 → 必須チェック、最大桁数チェック（10桁）、相関チェック_分類と商品
            {"is_required": True, "max_len": 10, "relation_setting_check": shurui_syohin_map, "relation_setting_check_index": "3"},
            # 項目名：商品注 → 最大桁数チェック（4桁）、相関チェック_分類と商品注
            {"max_len": 4, "relation_setting_check": shurui_syohinchu_map, "relation_setting_check_index": "3"},
            # 項目名：店番 → 最大桁数チェック（3桁）、文字種チェック（半角英数字）
            {"max_len": 3, "type": "alphanumeric"},
            # 項目名：属性 → 最大桁数チェック（2桁）、相関チェック_店番と属性
            {"max_len": 2, "is_required": True, "relation_setting_check": "6"},
            # 項目名：円換算済フラグ → 必須チェック、個別チェック_0または1チェック
            {"is_required": True},
            # 項目名：通貨 → 最大桁数チェック（3桁）、文字種チェック（半角英字）、相関チェック_円換算済フラグと通貨
            {"max_len": 3, "type": "alphanumeric", "relation_setting_check": yen_kansan_zumi_frg_index},
            # 項目名：粗利金額（全体） → 必須チェック、共通チェック_数値（17.0）
            {"is_required": True, "common_check": "number_17_0"},
            # 項目名：粗利金額（事業） → 必須チェック、共通チェック_数値（17.0）
            {"is_required": True, "common_check": "number_17_0"},
            # 項目名：備考 → 最大桁数チェック（100桁）
            {"max_len": 100}
        ]

        # 共通チェック部品（DFチェック）を呼び出して、データチェック処理を行う
        issues = checks.validate_dataframe(upload_df_after, list(validation_rules))

        # 処理終了する
        return issues, upload_df_after

def insert_to_work_table(upload_df: pd.DataFrame, uchiwake_cd: str, ym: str, torikomi_id: str) -> bool:
    """
    マーケット対顧取引粗利「実行」ボタン押下イベント
    upload_df(pandas.DataFrame): アップロード情報
    uchiwake_cd(str): 内訳コード
    ym(str): 基準年月
    torikomi_id(str): 取込ID
    Returns:
        bool データ登録結果
    """

    # テーブル（WORK_証券代行粗利）に既に登録されているデータの存在チェックを行う
    result_list = runner.query(
        "get_t456uwoa030",
        {
            "uchiwake_code": uchiwake_cd,
            "ym": ym
        },
        True
    )

    # 想定以外エラーの場合
    error_msg = st.session_state.get(SESSION_KEY.SS_KEY_DB_ERROR, None)
    if error_msg:
        return False

    # 既に登録されているデータが存在する場合
    result_list = result_list.to_pandas()
    if len(result_list) == 1 and result_list.loc[0, "DATA_COUNT"] > 0:
        # 既に登録されているデータを削除する
        runner.execute(
            "del_t456uwoa030",
            {
                "uchiwake_code": uchiwake_cd,
                "ym": ym
            },
            True
        )

    # 想定以外エラーの場合
    error_msg = st.session_state.get(SESSION_KEY.SS_KEY_DB_ERROR, None)
    if error_msg:
        return False

    # アップロード情報のカラム名をテーブル定義書の項目名（物理名）に変換する
    upload_df = upload_df.rename(columns=column_map)

    # アップロード情報内の空値をNoneに変換する
    upload_df = upload_df.replace("", None)

    # 内訳コード、ファイル内連番および取込IDを列としてアップロード情報に追加する
    upload_df["UCHIWAKE_CD"] = uchiwake_cd
    upload_df["FILENAI_RENBAN"] = upload_df.index + 1
    upload_df["TORIKOMI_ID"] = torikomi_id

    # データを登録する
    insert_count = runner.vectorized_insert(
        upload_df,
        schema='UPLOAD', # スキーマ名
        table_name='T456UWOA030', # テーブル名
        overwrite=False, # 既存データクリアしない
        has_transaction = True
    )

    # 想定以外エラーの場合
    error_msg = st.session_state.get(SESSION_KEY.SS_KEY_DB_ERROR, None)
    if error_msg:
        return False

    # データ登録結果を確認する
    if insert_count != len(upload_df):
        return False
    else:
        return True

def clean_data(upload_df: pd.DataFrame) -> pd.DataFrame:
    """
    データクレンジング処理
    upload_df(pandas.DataFrame): アップロード情報
    Returns:
        pandas.DataFrame クレンジング後のアップロード情報
    """

    # クレンジング種類
    # 前後空白削除対象リスト
    trim_cols = [
        "年月",
        "法人番号",
        "取引先名",
        "分類",
        "商品",
        "商品注",
        "店番",
        "属性",
        "円換算済フラグ",
        "通貨",
        "備考"
    ]

    # 前後の空白を削除する
    upload_df[trim_cols] = upload_df[trim_cols].apply(lambda x: x.str.strip())

    return upload_df
