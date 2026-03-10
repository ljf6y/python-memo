■チェックルール定義

引数 rules に設定可能なチェックルールは以下の通りとする。

ルール名	説明	設定例
is_required	必須チェック	"is_required": True
max_len	最大桁数チェック	"max_len": 6
type	文字種チェック	"type": "alphanumeric"
common_check	共通チェック	"common_check": "yyyymmdd"
list_check	リスト範囲チェック	"list_check": ["A","B"]
db_check	DB存在チェック	"db_check": "hojin_number"
kijunym_check	基準年月チェック	"kijunym_check": "kijundate"
private_check	個別チェック	"private_check": "xxx_check"
