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



rules = {

#項目名：年月 → 必須、基準年月チェック、年月チェック
0 : {
    "is_required": True,
    "kijunym_check": "kijundate",
    "common_check": "yyyymm"
},

#項目名：粗利種類コード → 必須、最大桁数6桁、英数字
1 : {
    "is_required": True,
    "max_len": 6,
    "type": "alphanumeric"
},

#項目名：セグメントコード → 最大桁数3桁、英字
2 : {
    "max_len": 3,
    "type": "alphabet"
},

#項目名：法人番号 → 最大桁数6桁、数字、DB存在チェック
3 : {
    "max_len": 6,
    "type": "numeric",
    "db_check": "hojin_number"
},

#項目名：取引先名 → 最大桁数100
4 : {
    "max_len": 100
},

#項目名：ステータス → リスト範囲チェック
5 : {
    "list_check": ["A","B","C"]
},

#項目名：金額 → 数値チェック（整数10桁、小数2桁）
6 : {
    "common_check": "number_10_2"
},

#項目名：個別チェック
7 : {
    "private_check": "custom_validation"
}

}


以下更新



チェック項目
rulesキー
型
説明
必須チェック
is_required
bool
値が未入力の場合エラーとする
最大桁数チェック
max_len
int
指定された桁数を超える場合エラー
文字種チェック
type
string
文字種チェック（numeric / alphabet / alphanumeric）
共通チェック
common_check
string
年月日・年月・年等の形式チェック
リスト範囲チェック
list_check
list
指定されたリスト内の値かチェック
DB存在チェック
db_check
string
指定されたマスタに存在するかチェック
基準年月チェック
kijunym_check
string
基準年月との比較チェック
個別チェック
private_check
string
機能固有の個別チェック




common_check値
型
説明
yyyymmdd
string
年月日チェック
yyyymm
string
年月チェック
yyyy
string
年チェック
number_x_y
string
数値チェック（整数x桁、小数y桁）









