--DA501.RISK_JOBTITLE_CODE AS STRING) AS INDUSTRY_TITLE,--職稱  代碼轉換
--共 3,754,896筆 record 
--共 3,754,896筆 不重複 ID_NO


--CREATE OR REPLACE TABLE dev_temp.data_analyst.ins_npbasic_tmp AS
WITH ins_npbasic_tmp AS (
--自然人集團客戶基本資料
SELECT
    CAST(CASE 
        WHEN DA501.IDNTFCTN_TYPE = '1' THEN 1 --身分證
        WHEN DA501.IDNTFCTN_TYPE = '2' THEN 2 --居留證
        ELSE 3
        END AS STRING) AS ID_TYPE,
    CAST(DA501.CUSTOMER_ID AS STRING) AS ID_NO, --身分證字號
    CAST('INS' AS STRING) AS SOURCE, --來源子公司別
    CAST(DA501.CUSTOMER_NAME AS STRING) AS C_FULLNAME, --中文姓名
    '' AS E_FULLNAME, --英文姓名 
    CAST(CASE WHEN DA501.SEX = 'M'  THEN '1'
              WHEN DA501.SEX = 'F'  THEN '2'
              ELSE '3' 
              END AS STRING) AS GENDER, --性別
    CAST(DA501.BIRTHDAY AS STRING) AS BIRTHDAY_DATE, --出生年月日 -- 全都是遮罩格式不符合
    '' AS EXPIRED_DATE, --居留證到期日 
    '' AS PASSPORT_NUMBER, --護照號碼
    '' AS PASSPORT_VALIDITY, --護照號碼到期日
    CAST(DA501.NATIONAL_COUNTRY_CODE AS STRING) AS COUNTRY, --國籍 --兩碼英文代碼
    --CAST(REGISTERED_ADDR.ADDRESS AS STRING) AS REGISTERED_ADDR, --戶籍地址 --等DTABP005落檔後補邏輯
    '' AS O_NAME, --別名
    '' AS EDUCATION_CODE,--教育程度  -- 只有兩筆是4
    CAST(DA501.OCCU_TYPE1_CODE AS STRING) AS INDUSTRY_TYPE1,--行職業大項 --以兩碼數字呈現 --需要代碼轉換config
    CAST(DA501.OCCU_TYPE3_CODE AS STRING) AS INDUSTRY_TYPE2,--行職業小項 --以七碼數字呈現 --需要代碼轉換config
    '' AS SERVICES,--任職機構
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
    END AS INDUSTRY_TITLE,--職稱  
    TRY_CAST(INCOME_SOURCE_ANNUAL_INCOME.PAY_SOURCE AS STRING) AS INVEST_SOURCE,--資產來源 
    '' AS ESTATE_SOURCE,--資金來源
    TRY_CAST(INCOME_SOURCE_ANNUAL_INCOME.MIN_YEAR_INCOME AS STRING) AS ANNUAL_INCOME, --年收入 
    '' AS PRODUCT_CUB,--建立業務目的
    CAST(CURRENT_DATE() AS TIMESTAMP_NTZ) AS LAST_MODIFIED_DATE,
    CAST('2024-08-16' AS TIMESTAMP_NTZ) AS CREATE_DATE--這裡要改成首次建立的日期
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

---------- 資產來源,年收入-----------
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
            MAX_BY(DP707.MIN_YEAR_INCOME, DP707.MIN_YEAR_INCOME) AS MIN_YEAR_INCOME
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
FROM ins_npbasic_tmp
