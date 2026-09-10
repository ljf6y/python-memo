

SELECT
    TABLE_CATALOG AS DATABASE_NAME,
    TABLE_SCHEMA AS SCHEMA_NAME,
    TABLE_NAME AS PHYSICAL_NAME,
    COMMENT AS LOGICAL_NAME
FROM MAIN_J.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA'
  AND TABLE_NAME IN (
      'T456SWMC080',
      'T456SWOF150',
      'T456SMMC020'
  )
ORDER BY TABLE_NAME;








メッセージキュー内のメッセージを順に処理し、各メッセージからコンテンツとレベルを取得する。レベル情報に基づいて、Streamlitの表示機能を決定する



アップロードファイル名には、ファイルパスを含め




ず、ファイル名のみを記載してください。