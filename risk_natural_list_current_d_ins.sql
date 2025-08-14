--共 3,754,896筆 record
--共 3,754,896筆 不重複 ID_NO

--RISK_LEVEL	客戶風險評級	1 低風險, 2 中風險, 3 高風險, 4 制裁
--IS_SAN, IS_PROHIBITEDLIST, IS_SIP, IS_AME, IS_PEP, IS_RCA    1:是, 0:否
--INDUSTRY_RISK	行職業風險等級	1 低風險, 2 中風險, 3 高風險
--    目前僅有：00低風險, 30高風險
--PROD_RISK_CXL_APC	產品風險評級	1 低風險, 2 中風險, 3 高風險
--COUNTRY_RISK	國家(地域)風險評級	1 低風險, 2 中風險, 3 高風險, 4 制裁
--    目前僅有：10低風險, 20中風險, 25中高風險, 30高風險, 40制裁

--CREATE OR REPLACE TABLE dev_temp.data_analyst.ins_nprisk_tmp AS
WITH ins_nprisk_tmp AS (
--自然人集團客戶風險資料
SELECT
    CAST(CASE 
        WHEN DA501.IDNTFCTN_TYPE = '1' THEN 1 --身分證
        WHEN DA501.IDNTFCTN_TYPE = '2' THEN 2 --居留證
        ELSE 3
        END AS STRING) AS ID_TYPE,
    CAST(DA501.CUSTOMER_ID AS STRING) AS ID_NO, --身分證字號
    CAST('INS' AS STRING) AS SOURCE, --來源子公司別
    CAST(
        CASE 
            WHEN DA501.RISK_LEVEL = '00' THEN '1' --低風險
            WHEN DA501.RISK_LEVEL = '30' THEN '3' --高風險
            ELSE DA501.RISK_LEVEL
        END AS STRING
    ) AS RISK_LEVEL, --客戶風險評級 --00, 30
    CAST(DA501.RISK_LEVEL_UPDATE AS STRING) AS RISK_MODIFY_DATE, --風險評級最後異動日期
    CAST(0 AS STRING) AS IS_CFH_PROHIBITEDLIST, 
    CAST(0 AS STRING) AS IS_CFH_SIP, 
    CAST(COALESCE(DA501.SAN, 0) AS STRING)AS IS_SAN, --制裁名單 -- 全 null
    CAST(COALESCE(DA501.MNY_ERR, 0) AS STRING) AS IS_PROHIBITEDLIST, --子公司禁止名單 -- null 跟 1
    CAST(COALESCE(DA501.SIP, 0) AS STRING) AS IS_SIP, --子公司關注名單 -- null 跟 1
    CAST(COALESCE(DA501.AM, 0) AS STRING) AS IS_AME, --子公司負面新聞 -- null 跟 1
    CAST(COALESCE(DA501.PEP, 0) AS STRING) AS IS_PEP, --重要政治人物名單(PEP) -- null 跟 1
    CAST(COALESCE(DA501.RCA, 0) AS STRING) AS IS_RCA, --重要政治人物名單_密切關係人名單(RCA) -- null 跟 1
    CAST(
        CASE 
            WHEN DG011.DATA1 = '00' THEN '1' --低風險
            WHEN DG011.DATA1 = '30' THEN '3' --高風險
            ELSE DG011.DATA1
        END AS STRING
    ) AS INDUSTRY_RISK, --行職業風險等級
    '' AS PROD_CXL_APC, --產品類別 --等第十八張表 DTATA579 落檔後補
    '' AS PROD_RISK_CXL_APC, --產品風險評級 --等第十八張表 DTATA579 落檔後補
    CAST(
        CASE 
            WHEN DG012.RISK_LEVEL = '10' THEN '1' --低風險
            WHEN DG012.RISK_LEVEL = '20' THEN '2' --中風險
            WHEN DG012.RISK_LEVEL = '25' THEN '2' --中高風險  ????????
            WHEN DG012.RISK_LEVEL = '30' THEN '3' --高風險
            WHEN DG012.RISK_LEVEL = '90' THEN '4' --制裁
            ELSE DG012.RISK_LEVEL
        END AS STRING
    ) AS COUNTRY_RISK, --國家(地域)風險評級
    CAST(CURRENT_DATE() AS TIMESTAMP_NTZ) AS LAST_MODIFIED_DATE,
    CAST('2024-08-16' AS TIMESTAMP_NTZ) AS CREATE_DATE --這裡要改成首次建立的日期
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

----------行職業風險等級---------
LEFT JOIN (
    SELECT PARAM_ID, DISPLAY_VALUE, DATA1
    FROM raw_clean.ins.DTAGG011
    WHERE PARAM_ID = 'RISK_INDUSTRY_TYPE'
) AS DG011
ON DA501.RISK_INDUSTRY_CODE = DG011.DISPLAY_VALUE

----------國家(地域)風險評級---------
LEFT JOIN raw_clean.ins.DTAGG012 AS DG012
ON DA501.OCR_COUNTRY_CODE = DG012.COUNTRY_CODE 

)

SELECT *
FROM ins_nprisk_tmp