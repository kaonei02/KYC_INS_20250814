--共 404,080,821筆 record
--共   3,019,066筆 不重複 ID_NO


--CREATE OR REPLACE TABLE dev_temp.data_analyst.ins_nprel_tmp AS
WITH ins_nprel_tmp AS (
-- 自然人集團客戶關聯人資料
SELECT
    CAST(CASE 
        WHEN DA501.IDNTFCTN_TYPE = '1' THEN 1 --身分證
        WHEN DA501.IDNTFCTN_TYPE = '2' THEN 2 --居留證
        ELSE 3
        END AS STRING) AS ID_TYPE,                     -- 證件類型
    CAST(DA501.CUSTOMER_ID AS STRING) AS ID_NO,        -- 身分證字號
    CAST('INS' AS STRING) AS SOURCE,                   -- 來源子公司別
    CAST(REL.RELATION_TYPE AS STRING) AS RELATION_TYPE,-- 關聯人種類
    CAST(REL.C_FULLNAME AS STRING) AS C_FULLNAME,      -- 中文全名
    CAST(REL.RELATION_CODE AS STRING) AS RELATION_CODE,-- 與客戶關係
    CAST(REL.BIRTHDAY_DATE AS STRING) AS BIRTHDAY_DATE,-- 出生年月日
    CAST(REL.RELATION_ID_NO AS STRING) AS RELATION_ID_NO, -- 關聯人身分證號/ID/證件號
    CAST(REL.COUNTRY AS STRING) AS COUNTRY,            -- 國籍
    CAST(CURRENT_DATE() AS TIMESTAMP_NTZ) AS LAST_MODIFIED_DATE,
    CAST('2024-08-16' AS TIMESTAMP_NTZ) AS CREATE_DATE,-- 首次建立的日期
    CAST(CASE 
        WHEN SUBSTRING(DA501.BIRTHDAY, 4, 1) RLIKE '[0-9]' THEN SUBSTRING(DA501.BIRTHDAY, 4, 1)
        ELSE 0
    END AS STRING) AS PTN
FROM(
    SELECT *
    FROM(
        SELECT *, ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY CHANGE_DATE_TIME DESC) AS rn
        FROM raw_clean.ins.DTATA501
        WHERE IDNTFCTN_TYPE IN ('1', '2', '3')
        AND CUSTOMER_ID IN ( 
            SELECT DISTINCT CUSTOMER_ID
            FROM raw_clean.ins.DTABP001 DP001
            JOIN raw_clean.ins.DTABP004 DP004
            ON DP001.CONTRACT_NO = DP004.CONTRACT_NO
            WHERE DP001.POLICY_STATUS = '7'
            AND (DATE(DP001.END_DATETIME) >= CURRENT_DATE() OR DP001.END_DATETIME IS NULL)
        )  --取出有效客戶
    )
    WHERE rn = 1 -- 取出每個 ID 最新一筆資料
) AS DA501

-- 業務員/理專、法定代理人、被保人、受益人
INNER JOIN (
    -- 業務員關聯資料
    SELECT DISTINCT
        CAST(DA501.CUSTOMER_ID AS STRING) AS ID,           -- 客戶ID
        CAST(1 AS INT) AS RELATION_TYPE,                   -- 1表示業務員
        CAST(NULL AS STRING) AS C_FULLNAME,                -- 客戶名稱
        CAST(NULL AS STRING) AS RELATION_CODE,             -- 與客戶關係(業務員關聯不需要)
        CAST(NULL AS STRING) AS BIRTHDAY_DATE,             -- 出生年月日
        CAST(DP002.AGENT_ID AS STRING) AS RELATION_ID_NO,  -- 業務員ID
        CAST(NULL AS STRING) AS COUNTRY                    -- 國籍
    FROM raw_clean.ins.DTABP002 DP002
    LEFT JOIN raw_clean.ins.DTATA501 DA501
        ON DP002.CONTRACT_NO = DA501.CONTRACT_NO

    UNION ALL
/*
    -- 被保險人關聯資料
    -- 左表約 2,000 萬筆，
    -- 右表同一 CONTRACT_NO 可能多筆（最多 600+），平均只要每筆左表匹配到 ~20 筆右表，
    -- LEFT JOIN 後就會膨脹到約 4 億（2,000 萬 × 20 ≈ 4 億）。
    SELECT DISTINCT
        CAST(DA501.CUSTOMER_ID AS STRING) AS ID,               -- 客戶ID
        CAST(4 AS INT) AS RELATION_TYPE,                       -- 4表示被保險人
        CAST(DP004.CUSTOMER_NAME AS STRING) AS C_FULLNAME,     -- 被保險人姓名
        CAST(NULL AS STRING) AS RELATION_CODE,                 -- 與客戶關係(不需要)
        CAST(DP004.BIRTHDAY AS STRING) AS BIRTHDAY_DATE,       -- 出生年月日
        CAST(DP004.CUSTOMER_ID AS STRING) AS RELATION_ID_NO,   -- 證件號
        CAST(NULL AS STRING) AS COUNTRY                        -- 國籍
    FROM raw_clean.ins.DTABP004 DP004
    LEFT JOIN raw_clean.ins.DTATA501 DA501
        ON DP004.CONTRACT_NO = DA501.CONTRACT_NO
    WHERE DA501.CUSTOMER_ID IS NOT NULL

    UNION ALL
*/
    -- 產險補充受益人只有健傷險和旅綜險有留存資料
    -- 健傷險受益人關聯資料
    SELECT DISTINCT
        CAST(DA501.CUSTOMER_ID AS STRING) AS ID,                              -- 客戶ID
        CAST(5 AS INT) AS RELATION_TYPE,                                      -- 5表示受益人
        CAST(DP606.ASSURED_NAME AS STRING) AS C_FULLNAME,                     -- 受益人姓名
        CAST(DP606.RELATIVE_TO_INSURED AS STRING) AS RELATION_CODE,           -- 與被保險人關係
        CAST(DA501_BIRTHDAY_NATIONAL.BIRTHDAY AS STRING) AS BIRTHDAY_DATE,    -- 出生年月日
        CAST(DP606.ASSURED_ID AS STRING) AS RELATION_ID_NO,                   -- 證件號
        CAST(DA501_BIRTHDAY_NATIONAL.NATIONAL_COUNTRY_CODE AS STRING) AS COUNTRY -- 國籍
    FROM raw_clean.ins.DTABP606 DP606
    LEFT JOIN raw_clean.ins.DTATA501 DA501
        ON DP606.CONTRACT_NO = DA501.CONTRACT_NO
    LEFT JOIN raw_clean.ins.DTATA501 DA501_BIRTHDAY_NATIONAL
        ON DP606.ASSURED_ID = DA501_BIRTHDAY_NATIONAL.CUSTOMER_ID

    UNION ALL

    -- 旅綜險受益人關聯資料
    SELECT DISTINCT
        CAST(DA501.CUSTOMER_ID AS STRING) AS ID,                        -- 客戶ID
        CAST(5 AS INT) AS RELATION_TYPE,                                -- 5表示受益人
        CAST(DP703.ASSURED_NAME AS STRING) AS C_FULLNAME,               -- 受益人姓名
        CAST(DP703.RELATIVE_TO_INSURED AS STRING) AS RELATION_CODE,     -- 與被保險人關係
        CAST(NULL AS STRING) AS BIRTHDAY_DATE,                          -- 出生年月日(無法取得)
        CAST(NULL AS STRING) AS RELATION_ID_NO,                         -- 證件號(無法取得)
        CAST(NULL AS STRING) AS COUNTRY                                 -- 國籍(無法取得)
    FROM raw_clean.ins.DTPAP703 DP703
    LEFT JOIN raw_clean.ins.DTATA501 DA501
        ON DP703.CONTRACT_NO = DA501.CONTRACT_NO

) REL
ON DA501.CUSTOMER_ID = REL.ID
)

SELECT *
FROM ins_nprel_tmp