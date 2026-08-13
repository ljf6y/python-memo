# パーセンテージ形式の項目を百分率に変換する

    percentage_cols = [

        "コスト率",

        "ベースレート(外貨)",

        "ベースレート(円貨)"

    ]

    # Excelから取得した割合値を100倍して百分率に変換する

    upload_df[percentage_cols] = upload_df[percentage_cols] * 100