
def is_exact_length(val_str: str, target_digits: int) -> bool:
    """
    固定桁数チェック
    val_str(str):チェック対象データ（例：1000）
    target_digits(int):桁数（例：4）

    前提条件:
        val_strにNoneを渡さないこと。
        空文字列は桁数チェック対象外とする。

    Returns:
        bool チェック結果
        (True:指定桁数と一致または空文字列、
         False:指定桁数と不一致)
    """

    if (len(val_str) == target_digits) | (val_str is None or val_str == ""):
        return True
    else:
        return False