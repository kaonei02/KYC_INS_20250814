--共 875,224筆 record (執行時間1m)
--共 194,240筆 不重複 ID_NO (執行時間1m)

-- 業務員164,015筆 (不重複 163,971)
-- 負責人194,280筆 (不重複 194,240)
-- 被保人307,518筆 (不重複 163,971)
-- 受益人 15,133筆 (不重複  10,329)



--CREATE OR REPLACE TABLE dev_temp.data_analyst.ins_lprel_tmp AS
WITH ins_lprel_tmp AS (
--產險法人集團客戶關聯人資料
SELECT
    CAST(DA501.CUSTOMER_ID AS STRING) AS ID_NO,               --統一編號
    CAST('INS' AS STRING) AS SOURCE,                    --來源子公司別
    CAST(REL.RELATION_TYPE AS STRING) AS RELATION_TYPE, --關聯人種類 1 業務員/理專ID ; 2 代表人 ; 3 負責人  8 被保險人 ; 9 受益人
    CAST(REL.C_FULLNAME AS STRING) AS C_FULLNAME, --姓名
    CAST(REL.REPRESNT_POS AS STRING) AS REPRESNT_POS, --職稱
    CAST(REL.RELATION_CODE AS STRING) AS RELATION_CODE, --與客戶關係 1 配偶 ; 2 父母 ; 3 兄弟姊妹 ; 4 祖父母 ; 5 子女 ; 6 監護 ; 7 其他
    CAST(REL.BIRTHDAY_DATE AS STRING) AS BIRTHDAY_DATE, --出生年月日
    CAST(REL.RELATION_ID_NO AS STRING) AS RELATION_ID_NO, --關聯人身分證號/ID/證件號
    CAST(REL.COUNTRY AS STRING) AS COUNTRY, --國籍
    CAST(CURRENT_DATE() AS TIMESTAMP_NTZ) AS LAST_MODIFIED_DATE, --更新日期
    CAST('2024-08-16' AS TIMESTAMP_NTZ) AS CREATE_DATE --建立日期
FROM(
    SELECT *
    FROM(
        SELECT *, ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY CHANGE_DATE_TIME DESC) AS rn
        FROM raw_clean.ins.DTATA501
        WHERE IDNTFCTN_TYPE IN ('4')
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

--------1業務員、2代表人、3負責人、8被保人、9受益人--------
INNER JOIN (
    -- 業務員關聯資料
    SELECT DISTINCT
        CAST(DA501.CUSTOMER_ID AS STRING) AS ID,           -- 客戶ID
        CAST(1 AS INT) AS RELATION_TYPE,                   -- 1表示業務員
        CAST(NULL AS STRING) AS C_FULLNAME,                -- 客戶名稱
        CAST(NULL AS STRING) AS REPRESNT_POS,              -- 職稱
        CAST(NULL AS STRING) AS RELATION_CODE,             -- 與客戶關係(業務員關聯不需要)
        CAST(NULL AS STRING) AS BIRTHDAY_DATE,             -- 出生年月日
        CAST(DP002.AGENT_ID AS STRING) AS RELATION_ID_NO,  -- 業務員ID
        CAST(NULL AS STRING) AS COUNTRY                    -- 國籍
    FROM raw_clean.ins.DTABP002 DP002
    LEFT JOIN raw_clean.ins.DTATA501 DA501
        ON DP002.CONTRACT_NO = DA501.CONTRACT_NO

    UNION ALL

    -- 被保險人關聯資料
    SELECT DISTINCT
        CAST(DA501.CUSTOMER_ID AS STRING) AS ID,               -- 客戶ID
        CAST(8 AS INT) AS RELATION_TYPE,                       -- 8表示被保險人
        CAST(DP004.CUSTOMER_NAME AS STRING) AS C_FULLNAME,     -- 被保險人姓名
        CAST(NULL AS STRING) AS REPRESNT_POS,                  -- 職稱
        CAST(NULL AS STRING) AS RELATION_CODE,                 -- 與客戶關係(不需要)
        CAST(DP004.BIRTHDAY AS STRING) AS BIRTHDAY_DATE,       -- 出生年月日
        CAST(DP004.CUSTOMER_ID AS STRING) AS RELATION_ID_NO,   -- 證件號
        CAST(DA501_NATIONAL.NATIONAL_COUNTRY_CODE AS STRING) AS COUNTRY -- 國籍
    FROM raw_clean.ins.DTABP004 DP004
    LEFT JOIN raw_clean.ins.DTATA501 DA501
        ON DP004.CONTRACT_NO = DA501.CONTRACT_NO
    LEFT JOIN raw_clean.ins.DTATA501 DA501_NATIONAL
        ON DP004.CUSTOMER_ID = DA501_NATIONAL.CUSTOMER_ID

    UNION ALL

    -- 產險補充受益人只有健傷險和旅綜險有留存資料
    -- 健傷險受益人關聯資料
    SELECT DISTINCT
        CAST(DA501.CUSTOMER_ID AS STRING) AS ID,                              -- 客戶ID
        CAST(9 AS INT) AS RELATION_TYPE,                                      -- 9表示受益人
        CAST(DP606.ASSURED_NAME AS STRING) AS C_FULLNAME,                     -- 受益人姓名
        CAST(NULL AS STRING) AS REPRESNT_POS,                                 -- 職稱
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
        CAST(9 AS INT) AS RELATION_TYPE,                                -- 9表示受益人
        CAST(DP703.ASSURED_NAME AS STRING) AS C_FULLNAME,               -- 受益人姓名
        CAST(NULL AS STRING) AS REPRESNT_POS,                           -- 職稱
        CAST(DP703.RELATIVE_TO_INSURED AS STRING) AS RELATION_CODE,     -- 與被保險人關係
        CAST(NULL AS STRING) AS BIRTHDAY_DATE,                          -- 出生年月日(無法取得)
        CAST(NULL AS STRING) AS RELATION_ID_NO,                         -- 證件號(無法取得)
        CAST(NULL AS STRING) AS COUNTRY                                 -- 國籍(無法取得)
    FROM raw_clean.ins.DTPAP703 DP703
    LEFT JOIN raw_clean.ins.DTATA501 DA501
        ON DP703.CONTRACT_NO = DA501.CONTRACT_NO

    UNION ALL

    -- 代表人關聯資料2
    SELECT DISTINCT
        CAST(DA501.CUSTOMER_ID AS STRING) AS ID,                              -- 客戶ID
        CAST(2 AS INT) AS RELATION_TYPE,                                      -- 2表示代表人
        CAST(COALESCE(DA525.MANAGER_NAME, DA501.MANAGER_NAME) AS STRING) AS C_FULLNAME,-- 受益人姓名
        CAST(
            CASE DA501.RISK_JOBTITLE_CODE
            WHEN 'T010' THEN '一般職員'
            WHEN 'T020' THEN '單位主管(不含財務單位)'
            WHEN 'T030' THEN '協理'
            WHEN 'T040' THEN '副總經理'
            WHEN 'T050' THEN '企業負責人'
            WHEN 'T060' THEN '董事、監察人'
            WHEN 'T070' THEN '財務主管(含外國企業在本地所設分公司之財務主管)'
            WHEN 'T080' THEN '總經理/執行長(含外國企業在本地所設分公司之General Manager)'
            WHEN 'T090' THEN '有權代表公司簽章人員'
            WHEN 'T100' THEN '院長'
            WHEN 'T110' THEN '校長'
            ELSE DA501.RISK_JOBTITLE_CODE
            END AS STRING
        ) AS REPRESNT_POS,                                                    -- 職稱
        CAST(NULL AS STRING) AS RELATION_CODE,                                -- 與被保險人關係
        CAST(DA525.BIRTHDAY AS STRING) AS BIRTHDAY_DATE,                      -- 出生年月日
        CAST(DA501.MANAGER_ID AS STRING) AS RELATION_ID_NO,                   -- 證件號
        CAST(DA525.NATIONAL_COUNTRY_CODE AS STRING) AS COUNTRY                -- 國籍
    FROM raw_clean.ins.DTATA501 DA501
    LEFT JOIN raw_clean.ins.DTATA525 DA525
        ON DA501.IDNTFCTN_TYPE = DA525.IDNTFCTN_TYPE 
        AND DA501.CUSTOMER_ID = DA525.CUSTOMER_ID

    UNION ALL

    -- 負責人關聯資料3 
    SELECT DISTINCT
        CAST(DA501.CUSTOMER_ID AS STRING) AS ID,                              -- 客戶ID
        CAST(3 AS INT) AS RELATION_TYPE,                                      -- 3表示負責人
        CAST(COALESCE(DA525.MANAGER_NAME, DA501.MANAGER_NAME) AS STRING) AS C_FULLNAME,-- 受益人姓名
        CAST(
            CASE DA501.RISK_JOBTITLE_CODE
            WHEN 'T010' THEN '一般職員'
            WHEN 'T020' THEN '單位主管(不含財務單位)'
            WHEN 'T030' THEN '協理'
            WHEN 'T040' THEN '副總經理'
            WHEN 'T050' THEN '企業負責人'
            WHEN 'T060' THEN '董事、監察人'
            WHEN 'T070' THEN '財務主管(含外國企業在本地所設分公司之財務主管)'
            WHEN 'T080' THEN '總經理/執行長(含外國企業在本地所設分公司之General Manager)'
            WHEN 'T090' THEN '有權代表公司簽章人員'
            WHEN 'T100' THEN '院長'
            WHEN 'T110' THEN '校長'
            ELSE DA501.RISK_JOBTITLE_CODE
            END AS STRING
        ) AS REPRESNT_POS,                                                    -- 職稱
        CAST(NULL AS STRING) AS RELATION_CODE,                                -- 與被保險人關係
        CAST(DA525.BIRTHDAY AS STRING) AS BIRTHDAY_DATE,                      -- 出生年月日
        CAST(DA501.MANAGER_ID AS STRING) AS RELATION_ID_NO,                   -- 證件號
        CAST(DA525.NATIONAL_COUNTRY_CODE AS STRING) AS COUNTRY                -- 國籍
    FROM raw_clean.ins.DTATA501 DA501
    LEFT JOIN raw_clean.ins.DTATA525 DA525
        ON DA501.IDNTFCTN_TYPE = DA525.IDNTFCTN_TYPE 
        AND DA501.CUSTOMER_ID = DA525.CUSTOMER_ID


) REL
ON DA501.CUSTOMER_ID = REL.ID
)

SELECT *
FROM ins_lprel_tmp