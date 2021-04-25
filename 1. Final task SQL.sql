--  Данная работа подразумевает сбора, загрузки, обработки и преобразования данных в конечный,
--  удобный для анализа вид, в форме итоговой витрины-отчета в базе данных.
--  ВАЖНО! данные которые попадают в первоначальные таблицы из Python после обработки удаляются.
--  Цель удаления: подготовка к новой загрузке. Накопление данных происходит у третьей стороны.


Create table C_Customer_Karateev_VG
(
    Customer_ID     VARCHAR2(20) not null
    ,Customer_Name   VARCHAR2(20)
    ,Segment_P       VARCHAR2(20)
    ,Country         VARCHAR2(20)
    ,City            VARCHAR2(20)
    ,State_C         VARCHAR2(20)
    ,Postal_Code     NUMBER(6,0)
    ,Region          VARCHAR2(20)
);
--////////////////////////////////////////////////
Create table C_Product_Karateev_VG
(
    Product_ID      VARCHAR2(20) not null
    ,Categor_c       VARCHAR2(20)
    ,Sub_Category    VARCHAR2(20)
    ,Product_Name    VARCHAR2(140 BYTE)
);
--////////////////////////////////////////////////
Create table C_orders_Karateev_VG
(
    Order_ID        VARCHAR2(20) not null
    ,Order_Date      DATE
    ,Ship_Date       DATE
    ,Ship_Mode       VARCHAR2(20)
);
--////////////////////////////////////////////////
Create table C_fin_Karateev_VG
(
    Row_ID          NUMBER(4,0)  not null
    ,Sales           NUMBER(10,5)
    ,Quantity        NUMBER(10,0)
    ,Discount        NUMBER(10,3)
    ,Profit          NUMBER(10,5)
    ,Order_ID        VARCHAR2(20)
    ,Customer_ID     VARCHAR2(20)
    ,Product_ID      VARCHAR2(20)
);
--////////////////////////////////////////////////
Create table B_Customer_Karateev_VG
(
    Customer_ID     VARCHAR2(20) not null
    ,Customer_Name   VARCHAR2(20)
    ,Segment_P       VARCHAR2(20)
    ,Country         VARCHAR2(20)
    ,City            VARCHAR2(20)
    ,State_C         VARCHAR2(20)
    ,Postal_Code     NUMBER(6,0)
    ,Region          VARCHAR2(20)
    ,CONSTRAINT B_Karateev_VG_pk_cust_id PRIMARY KEY (Customer_ID)
);
--////////////////////////////////////////////////
Create table B_Product_Karateev_VG
(
    Product_ID      VARCHAR2(20) not null
    ,Categor_c       VARCHAR2(20)
    ,Sub_Category    VARCHAR2(20)
    ,CONSTRAINT B_Karateev_VG_pk_prod_id PRIMARY KEY (product_id)
);
--////////////////////////////////////////////////
Create table B_orders_Karateev_VG
(
    Order_ID        VARCHAR2(20) not null
    ,Order_Date      DATE not null
    ,Ship_Date       DATE not null
    ,Ship_Mode       VARCHAR2(20)
    ,CONSTRAINT B_Karateev_VG_pk_order_id PRIMARY KEY (Order_ID)
);
--////////////////////////////////////////////////
Create table B_fin_Karateev_VG
(
    Row_ID          NUMBER(4,0)  not null
    ,Sales           NUMBER(10,5)
    ,Quantity        NUMBER(10,0)
    ,Discount        NUMBER(10,3)
    ,Profit          NUMBER(10,5)
    ,Order_ID        VARCHAR2(20)
    ,Customer_ID     VARCHAR2(20)
    ,Product_ID      VARCHAR2(20)
    ,CONSTRAINT B_Karateev_VG_pk_fin_id PRIMARY KEY (Row_ID)
    );
--////////////////////////////////////////////////
Create table  A_basic_shop_Karateev_VG
(
    Row_ID          NUMBER(4,0)
    ,Sales           NUMBER(10,5)
    ,Quantity        NUMBER(10,0)
    ,Discount        NUMBER(10,3)
    ,Profit          NUMBER(10,5)
    ,Order_ID        VARCHAR2(20)
    ,Customer_ID     VARCHAR2(20)
    ,Product_ID      VARCHAR2(20)
    ,Order_Date      DATE
    ,Ship_Date       DATE
    ,Ship_Mode       VARCHAR2(20)
    ,Customer_Name   VARCHAR2(20)
    ,Segment_P       VARCHAR2(20)
    ,Country         VARCHAR2(20)
    ,Categor_c       VARCHAR2(20)
    ,Sub_Category    VARCHAR2(20)
    ,CONSTRAINT A_basic_shop_VG_pk_BS_id PRIMARY KEY (Row_ID)
    );
--////////////////////////////////////////////////
Create table A_final_1_showcasev_VG
(
    DATE_1           DATE
    ,SHIP_MODE       VARCHAR2(20)
    ,Quantity        NUMBER(10,0)
    ,TOTAL           NUMBER(10,5)
);
--////////////////////////////////////////////////
Create table A_final_2_showcasev_VG
(
    DATE_2           DATE
    ,Customer_Name   VARCHAR2(20)
    ,Segment_P       VARCHAR2(20)
    ,TOTAL           NUMBER(10,5)
);
--////////////////////////////////////////////////
Create table A_final_3_showcasev_VG
(
    DATE_3           DATE
    ,Categor_c       VARCHAR2(20)
    ,Sub_Category    VARCHAR2(20)
    ,Quantity        NUMBER(10,0)
    ,TOTAL           NUMBER(10,3)
);
--////////////////////////////////////////////////
/
CREATE OR REPLACE PACKAGE PCG_KVG_1
IS
    PROCEDURE KARATEEV_VG_ADD;

END PCG_KVG_1;
/
CREATE OR REPLACE PACKAGE BODY PCG_KVG_1
AS
    PROCEDURE KARATEEV_VG_ADD
    IS

            CURSOR B_Customer_cursor IS SELECT    -- Выборка для таблицы № 1 "Customer"
                    UNIQUE (Customer_ID)
                    ,Customer_Name
                    ,Segment_P
                    ,Country
                FROM C_Customer_Karateev_VG
                ORDER BY 1;
                    v_Customer_ID    VARCHAR2 (10);
                    v_Customer_Name  VARCHAR2 (20);
                    v_Segment_P      VARCHAR2 (20);
                    v_Country        VARCHAR2 (20);
                    v1_ct             INTEGER := 0;
        --------------------------------------------------------------  
            CURSOR B_Product_cursor IS SELECT     -- Выборка для таблицы № 2 "Product"
                    UNIQUE (PRODUCT_ID)
                    ,CATEGOR_C
                    ,SUB_CATEGORY
                FROM C_Product_Karateev_VG ORDER BY 1;
                    v_PRODUCT_ID    VARCHAR2 (20);
                    v_CATEGOR_C     VARCHAR2 (20);
                    v_SUB_CATEGORY  VARCHAR2 (20);
                    v2_ct            INTEGER := 0;
        --------------------------------------------------------------  
            CURSOR B_orders_cursor IS SELECT     -- Выборка для таблицы № 3 "Orders"
                    UNIQUE (ORDER_ID)
                    ,ORDER_DATE
                    ,SHIP_DATE
                    ,SHIP_MODE
                FROM C_orders_Karateev_VG ORDER BY 1;
                    v_ORDER_ID      VARCHAR2 (20);
                    v_ORDER_DATE    DATE;
                    v_SHIP_DATE     DATE;
                    v_SHIP_MODE     VARCHAR2 (20);
                    v3_ct            INTEGER := 0;
        --------------------------------------------------------------  
            CURSOR B_FIN_cursor IS SELECT       -- Выборка для таблицы № 4 "FIN"
                    DISTINCT (Row_ID)
                    ,Sales
                    ,Quantity
                    ,Discount
                    ,Profit
                    ,Order_ID
                    ,Customer_ID
                    ,Product_ID
            FROM C_fin_Karateev_VG ORDER BY 1;
                    v_Row_ID          NUMBER(4,0);
                    v_Sales           NUMBER(10,5);
                    v_Quantity        NUMBER(10,0);
                    v_Discount        NUMBER(10,3);
                    v_Profit          NUMBER(10,5);
                    v4_Order_ID        VARCHAR2(20);
                    v4_Customer_ID     VARCHAR2(20);
                    v4_Product_ID      VARCHAR2(20); 
                    v4_ct              INTEGER := 0;
        --------------------------------------------------------------          
            CURSOR A_basic_shop_cursor IS SELECT   -- Создание базовой витрины
                    k1.Row_ID
                    ,k1.Sales
                    ,k1.Quantity
                    ,k1.Discount
                    ,k1.Profit
                    ,k1.Order_ID
                    ,k1.Customer_ID
                    ,k1.Product_ID
                    ,k2.Order_Date
                    ,k2.Ship_Date
                    ,k2.Ship_Mode
                    ,k3.Customer_Name
                    ,k3.Segment_P
                    ,k3.Country
                    ,k4.Categor_c
                    ,k4.Sub_Category    
            FROM B_fin_Karateev_VG k1
                LEFT JOIN B_orders_Karateev_VG K2 ON k1.Order_ID = k2.Order_ID
                LEFT JOIN B_Customer_Karateev_VG K3 ON k1.Customer_ID = k3.Customer_ID
                LEFT JOIN B_Product_Karateev_VG K4 ON k1.Product_ID = k4.Product_ID
            ORDER BY 1;
                vk_row_id          NUMBER(4,0);
                vk_sales           NUMBER(10,5);
                vk_quantity        NUMBER(10,0);
                vk_discount        NUMBER(10,3);
                vk_profit          NUMBER(10,5);
                vk_order_id        VARCHAR2(20);
                vk_customer_id     VARCHAR2(20);
                vk_product_id      VARCHAR2(20);
                vk_order_date      DATE;
                vk_ship_date       DATE;
                vk_ship_mode       VARCHAR2(20);
                vk_customer_name   VARCHAR2(20);
                vk_segment_p       VARCHAR2(20);
                vk_country         VARCHAR2(20);
                vk_categor_c       VARCHAR2(20);
                vk_sub_category    VARCHAR2(20);
                v5_ct              INTEGER := 0;
        
        ----------------------------------------------------------------------------------------------------------------------------              
        BEGIN
                EXECUTE IMMEDIATE 'TRUNCATE TABLE B_Customer_Karateev_VG DROP STORAGE';              -- Очистка перед внесением новых данных таблицы № 1 "Customer"
                EXECUTE IMMEDIATE 'TRUNCATE TABLE B_Product_Karateev_VG DROP STORAGE';               -- Очистка перед внесением новых данных таблицы № 2 "Product"
                EXECUTE IMMEDIATE 'TRUNCATE TABLE B_orders_Karateev_VG DROP STORAGE';                -- Очистка перед внесением новых данных таблицы № 3 "Orders"
                EXECUTE IMMEDIATE 'TRUNCATE TABLE B_fin_Karateev_VG DROP STORAGE';                   -- Очистка перед внесением новых данных таблицы № 4 "FIN"
                EXECUTE IMMEDIATE 'TRUNCATE TABLE A_basic_shop_Karateev_VG DROP STORAGE';            -- Очистка перед внесением новых данных базовой витрины
                EXECUTE IMMEDIATE 'TRUNCATE TABLE A_final_1_showcasev_VG DROP STORAGE';-- Очистка перед внесением новых данных итоговой витрины № 1
                EXECUTE IMMEDIATE 'TRUNCATE TABLE A_final_2_showcasev_VG DROP STORAGE';-- Очистка перед внесением новых данных итоговой витрины № 2
                EXECUTE IMMEDIATE 'TRUNCATE TABLE A_final_3_showcasev_VG DROP STORAGE';-- Очистка перед внесением новых данных итоговой витрины № 3
        --------------------------------------------------------------  
                OPEN B_Customer_cursor;    -- Вставка данных в таблицу №3 "Customer"
                    loop
                            FETCH B_Customer_cursor INTO v_Customer_ID, v_Customer_Name, v_Segment_P, v_Country;
                            EXIT WHEN B_Customer_cursor%NOTFOUND;
                            v1_ct := v1_ct + 1;
                            IF MOD (v1_ct, 5) = 0 THEN
                            COMMIT;
                            END IF;
                            INSERT INTO B_Customer_Karateev_VG (CUSTOMER_ID, CUSTOMER_NAME, SEGMENT_P, COUNTRY) VALUES (v_Customer_ID, v_Customer_Name, v_Segment_P, v_Country);
                    END LOOP;
                COMMIT;
                CLOSE B_Customer_cursor;
        --------------------------------------------------------------        
                OPEN B_Product_cursor;     -- Вставка данных в таблицу №2 "Product"
                    loop
                            FETCH B_Product_cursor INTO v_PRODUCT_ID, v_CATEGOR_C, v_SUB_CATEGORY;
                            EXIT WHEN B_Product_cursor%NOTFOUND;
                            v2_ct := v2_ct + 1;
                            IF MOD (v2_ct, 5) = 0 THEN
                            COMMIT;
                            END IF;
                            INSERT INTO B_Product_Karateev_VG (PRODUCT_ID, CATEGOR_C, SUB_CATEGORY) VALUES (v_PRODUCT_ID, v_CATEGOR_C, v_SUB_CATEGORY);
                    END LOOP;
                COMMIT;
                CLOSE B_Product_cursor;
        --------------------------------------------------------------  
                OPEN B_orders_cursor;     -- Вставка данных в таблицу №3 "Orders"
                    loop
                            FETCH B_orders_cursor INTO v_ORDER_ID, v_ORDER_DATE, v_SHIP_DATE, v_SHIP_MODE;
                            EXIT WHEN B_orders_cursor%NOTFOUND;
                            v3_ct := v3_ct + 1;
                            IF MOD (v3_ct, 5) = 0 THEN
                            COMMIT;
                            END IF;
                            INSERT INTO B_orders_Karateev_VG (ORDER_ID, ORDER_DATE, SHIP_DATE, SHIP_MODE)
                            VALUES (v_ORDER_ID, v_ORDER_DATE, v_SHIP_DATE, v_SHIP_MODE);
                    END LOOP;
                COMMIT;
                CLOSE B_orders_cursor;
        -------------------------------------------------------------- 
                OPEN B_FIN_cursor;       -- Вставка данных в таблицу №3 "FIN"
                    loop
                            FETCH B_FIN_cursor INTO v_ROW_ID, v_SALES, v_QUANTITY, v_DISCOUNT, v_PROFIT, v4_ORDER_ID, v4_CUSTOMER_ID, v4_PRODUCT_ID;
                            EXIT WHEN B_FIN_cursor%NOTFOUND;
                            v4_ct := v4_ct + 1;
                            IF MOD (v4_ct, 5) = 0 THEN
                            COMMIT;
                            END IF;
                            INSERT INTO B_fin_Karateev_VG (ROW_ID, SALES, QUANTITY, DISCOUNT, PROFIT, ORDER_ID, CUSTOMER_ID, PRODUCT_ID)
                            VALUES (v_ROW_ID, v_SALES, v_QUANTITY, v_DISCOUNT, v_PROFIT, v4_ORDER_ID, v4_CUSTOMER_ID, v4_PRODUCT_ID);
                    END LOOP;
                COMMIT;
                CLOSE B_FIN_cursor;
        -------------------------------------------------------------- 
               OPEN A_basic_shop_cursor; -- Вставка данных в базовую витрину.
                    loop
                            FETCH A_basic_shop_cursor INTO vk_row_id, vk_sales, vk_quantity, vk_discount, vk_profit, vk_order_id, vk_customer_id, vk_product_id, 
                                                           vk_order_date, vk_ship_date, vk_ship_mode, vk_customer_name, vk_segment_p, vk_country, vk_categor_c, vk_sub_category;
                            EXIT WHEN A_basic_shop_cursor%NOTFOUND;
                            v5_ct := v5_ct + 1;
                            IF MOD (v5_ct, 5) = 0 THEN
                            COMMIT;
                            END IF;
                            INSERT INTO A_basic_shop_Karateev_VG (ROW_ID,SALES,Quantity,DISCOUNT,PROFIT,ORDER_ID,CUSTOMER_ID,PRODUCT_ID,
                                                                  ORDER_DATE,SHIP_DATE,SHIP_MODE,CUSTOMER_NAME,SEGMENT_P,COUNTRY,CATEGOR_C,SUB_CATEGORY) 
                                                                  VALUES (vk_row_id, vk_sales, vk_quantity, vk_discount, vk_profit, vk_order_id, vk_customer_id, vk_product_id, 
                                                                          vk_order_date, vk_ship_date, vk_ship_mode, vk_customer_name, vk_segment_p, vk_country, vk_categor_c, vk_sub_category);
                    END LOOP;
            COMMIT;
            CLOSE A_basic_shop_cursor;
        -------------------------------------------------------------- 
            INSERT INTO A_final_1_showcasev_VG(DATE_1, ship_mode, Quantity, TOTAL) --Вставка данных в итоговую витрину № 1.
            SELECT
                Order_Date
                ,SHIP_MODE
                ,COUNT(Sales) as Quantity
                ,SUM(Sales) as TOTAL
            FROM A_basic_shop_Karateev_VG
                GROUP BY Order_Date, SHIP_MODE
                ORDER BY Order_Date, SHIP_MODE, TOTAL DESC;
            COMMIT;
        -------------------------------------------------------------- 
            INSERT INTO A_final_2_showcasev_VG(DATE_2, Customer_Name, Segment_P, TOTAL) --Вставка данных в итоговую витрину № 2.
            SELECT
                Order_Date
                ,Customer_Name
                ,Segment_P
                ,SUM(Sales) as SUMM
            FROM A_basic_shop_Karateev_VG
                GROUP BY Order_Date, Customer_Name, Segment_P
                ORDER BY SUMM DESC, Customer_Name, Segment_P;
            COMMIT;
        
        -------------------------------------------------------------- 
            INSERT INTO A_final_3_showcasev_VG(DATE_3, Categor_c, Sub_Category, Quantity, TOTAL)--Вставка данных в итоговую витрину № 3.
            SELECT
                Order_Date
                ,Categor_c
                ,Sub_Category
                ,COUNT(Sales) as Quantity
                ,SUM(Sales) as TOTAL
            FROM A_basic_shop_Karateev_VG
                GROUP BY Order_Date, Categor_c, Sub_Category
                ORDER BY Order_Date, TOTAL DESC;
            COMMIT;
        -------------------------------------------------------------- 
            EXECUTE IMMEDIATE 'TRUNCATE TABLE C_Customer_Karateev_VG DROP STORAGE';          -- Очистка начальной таблицы "С" № 1 "Customer". Подготовка к следующей загрузке.
            EXECUTE IMMEDIATE 'TRUNCATE TABLE C_Product_Karateev_VG DROP STORAGE';           -- Очистка начальной таблицы "С" № 2 "Product". Подготовка к следующей загрузке.
            EXECUTE IMMEDIATE 'TRUNCATE TABLE C_orders_Karateev_VG DROP STORAGE';            -- Очистка начальной таблицы "С" № 3 "Orders". Подготовка к следующей загрузке.
            EXECUTE IMMEDIATE 'TRUNCATE TABLE C_fin_Karateev_VG DROP STORAGE';               -- Очистка начальной таблицы "С" № 4 "FIN". Подготовка к следующей загрузке.
            
    END KARATEEV_VG_ADD;
END PCG_KVG_1;
