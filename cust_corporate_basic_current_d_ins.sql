--共 194,240筆 record
--共 194,240筆 不重複 ID_NO



--CREATE OR REPLACE TABLE dev_temp.data_analyst.ins_lpbasic_tmp AS
WITH ins_lpbasic_tmp AS (
--非自然人集團客戶基本資料
SELECT
    CAST(DA501.CUSTOMER_ID AS STRING) AS ID_NO, --統一編號
    CAST('INS' AS STRING) AS SOURCE, --來源子公司別
    CAST(DA501.CUSTOMER_NAME AS STRING) AS C_FULLNAME, --中文姓名
    '' AS E_FULLNAME, --英文姓名
    CAST(DA501.ENTITY_SETTIME AS STRING) AS BUILD_DATE, --公司設立日期*
    CAST(DA501.ENTITY_REG_CTYCODE AS STRING) AS COUNTRY, --註冊國別*
    '' AS REGISTERED_ADDR,
    --CAST(REGISTERED_ADDR.ADDRESS AS STRING) AS REGISTERED_ADDR, --註冊地址 --等這張表落檔後補
    CAST(DA501.OCCU_TYPE1_CODE AS STRING) AS INDUSTRY_TYPE1,--行職業大項
    CAST(DA501.OCCU_TYPE3_CODE AS STRING) AS INDUSTRY_TYPE2,--行職業小項
    '' AS ESTATE_SOURCE,--資金來源
    TRY_CAST(INCOME_SOURCE_ANNUAL_INCOME.PAY_SOURCE AS STRING) AS INVEST_SOURCE,--資產來源
    '' AS YEAR_PROFIT,--年營業額
    TRY_CAST(INCOME_SOURCE_ANNUAL_INCOME.MIN_YEAR_INCOME AS STRING) AS PAID_UP_CAPITAL,--實收資本額   
    '' AS PRODUCT_CUB,--建立業務目的
    '' AS OWNER_STRUCT,--股權結構三層以上  0 不是 ;1 一層 ;2 二層 ;3 三層(含)以上
    CAST(DA501.BEARER_SHARES_STATUS AS STRING) AS IS_BEARER_SHARES,--無記名股份或由無記名股份公司持股 1:是, 0:否
    '' AS IS_HIDE_OWNER,--具有隱名股東、Nominee之情況 1:是, 0:否
    CAST(CURRENT_DATE() AS TIMESTAMP_NTZ) AS LAST_MODIFIED_DATE, --更新日期
    CAST('2024-08-16' AS TIMESTAMP_NTZ) AS CREATE_DATE  --建立日期
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

/*
---------- 註冊地址----------
LEFT JOIN (
    SELECT * 
    FROM (
        SELECT CUSTOMER_ID, ADDRESS,
        ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY CREATE_DATE DESC) rn 
        FROM (
            SELECT CUSTOMER_ID, ADDRESS, CREATE_DATE
            FROM raw_clean.ins.DTABP005
            WHERE ADDRESS_KIND = '1'
            AND (ADDRESS <> ''  AND ADDRESS IS NOT NULL)
        ) r
    ) temp
    WHERE temp.rn = 1
) REGISTERED_ADDR   
ON DA501.CUSTOMER_ID = REGISTERED_ADDR.CUSTOMER_ID
*/

---------- 資產來源,實收資本額-----------
LEFT JOIN (
    SELECT CUSTOMER_ID, PAY_SOURCE, MIN_YEAR_INCOME
    FROM (
        SELECT 
            DA501.CUSTOMER_ID, 
            CONCAT_WS(',', COLLECT_SET(
                CASE DP707.PAY_SOURCE
                    WHEN '1' THEN '工作/營業收入'
                    WHEN '2' THEN '投資/業外收入'
                    WHEN '3' THEN '退休收入'
                    WHEN '4' THEN '貸款'
                    WHEN '5' THEN '解除或終止契約'
                    WHEN '6' THEN '保單借款'
                    WHEN '7' THEN '存款'
                    ELSE DP707.PAY_SOURCE
                END
            )) AS PAY_SOURCE,
            MAX_BY(DP707.MIN_YEAR_INCOME, DP707.CHANGE_DATE_TIME) AS MIN_YEAR_INCOME
        FROM raw_clean.ins.DTPAP707 DP707
        LEFT JOIN raw_clean.ins.DTATA501 DA501
            ON DP707.CONTRACT_NO = DA501.CONTRACT_NO
        WHERE ((DP707.PAY_SOURCE <> '' 
            AND DP707.PAY_SOURCE IS NOT NULL)
            OR (DP707.MIN_YEAR_INCOME IS NOT NULL))
            AND DA501.CUSTOMER_ID IS NOT NULL
        GROUP BY DA501.CUSTOMER_ID
        -- UNION ALL 待DTABP611落檔後補邏輯
    ) r
) INCOME_SOURCE_ANNUAL_INCOME
ON DA501.CUSTOMER_ID = INCOME_SOURCE_ANNUAL_INCOME.CUSTOMER_ID
)

SELECT *
FROM ins_lpbasic_tmp
