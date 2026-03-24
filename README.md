-- name: check_hojin_number
SELECT COUNT(*) AS CNT
FROM T456SMMD010
WHERE HOJIN_NO = :hojin_no
  AND KIJUN_YM = :kijun_ym
;
