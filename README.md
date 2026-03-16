-- name: get_t456uwoa040
SELECT COUNT(YM) AS DATA_COUNT
FROM MAIN_H.UPLOAD.T456UWOA040
WHERE
    UCHIWAKE_CD = '{{uchiwake_code}}'
    AND YM = '{{ym}}'


-- name: del_t456uwoa040
DELETE
FROM MAIN_H.UPLOAD.T456UWOA040
WHERE
    UCHIWAKE_CD = '{{uchiwake_code}}'
    AND YM = '{{ym}}'
