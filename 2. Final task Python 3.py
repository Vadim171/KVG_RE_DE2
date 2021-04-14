# -*- coding: utf-8 -*-
"""
Created on Sat Apr 07 21:15:24 2021

@author: Рабочий1
"""

import sqlalchemy as adb
from sqlalchemy import MetaData
import cx_Oracle as ora
import pandas as pd
import datetime as dt


l_user = 'Ввести логин'   # Логин удален по причине открытого доступа к проекту.
l_pass = 'ВВести Пароль' # Пароль удален по причине открытого доступа к проекту.
l_tns = ora.makedsn('13.95.167.129', 1521, service_name = 'pdb1')

l_conn_ora = adb.create_engine(r'oracle://{p_user}:{p_pass}@{p_tns}'.format(
    p_user = l_user
    , p_pass = l_pass
    , p_tns = l_tns
    )
    )

print(l_conn_ora)

l_meta = MetaData(l_conn_ora)
l_meta.reflect()
l_tab1 = l_meta.tables['c_customer_karateev_vg']
print(l_tab1)

l_meta = MetaData(l_conn_ora)
l_meta.reflect()
l_tab2 = l_meta.tables['c_product_karateev_vg']
print(l_tab2)

l_meta = MetaData(l_conn_ora)
l_meta.reflect()
l_tab3 = l_meta.tables['c_orders_karateev_vg']
print(l_tab3)

l_meta = MetaData(l_conn_ora)
l_meta.reflect()
l_tab4 = l_meta.tables['c_fin_karateev_vg']
print(l_tab4)

c = 0

l_file_excel = pd.read_excel(r'C:\1\Sample - Superstore.xlsx', sheet_name = 'Orders') # Требуется изменить путь к файлу.
l_list = l_file_excel.values.tolist()
for i in l_list:
    c +=1
    print(c)    
    l_tab1.insert([l_tab1.c.customer_id, l_tab1.c.customer_name, l_tab1.c.segment_p, l_tab1.c.country, 
                   l_tab1.c.city, l_tab1.c.state_c, l_tab1.c.postal_code, l_tab1.c.region]).values(
        customer_id      = i[5]
        ,customer_name   = i[6]
        ,segment_p       = i[7]
        ,country         = i[8]
        ,city            = i[9]
        ,state_c         = i[10]
        ,postal_code     = i[11]
        ,region          = i[12]
        ).execute()
       
    l_tab2.insert([l_tab2.c.product_id, l_tab2.c.categor_c, l_tab2.c.sub_category, l_tab2.c.product_name]).values(
        product_id	    = i[13]
        ,categor_c	    = i[14]
        ,sub_category	= i[15]
        ,product_name	= i[16]
        ).execute()
       
    l_tab3.insert([l_tab3.c.order_id, l_tab3.c.order_date, l_tab3.c.ship_date, l_tab3.c.ship_mode]).values(
        order_id	   = i[1]
        ,order_date	   = dt.datetime.date(i[2])
        ,ship_date	   = dt.datetime.date(i[3])
        ,ship_mode	   = i[4]
        ).execute()
       
    l_tab4.insert([l_tab4.c.row_id, l_tab4.c.sales, l_tab4.c.quantity, l_tab4.c.discount, l_tab4.c.profit, l_tab4.c.order_id, l_tab4.c.customer_id, l_tab4.c.product_id]).values(
        row_id	       = i[0]
        ,sales		   = i[17]
        ,quantity	   = i[18]
        ,discount	   = i[19]
        ,profit		   = i[20]
        ,order_id	   = i[1]
        ,customer_id   = i[5]
        ,product_id	   = i[13]
        ).execute()

print('Запуск процедуры')

l_conn_ora.execute(adb.text('BEGIN pcg_kvg_1.karateev_vg_add; END;'))

print('Готово')
print('''В вашем распоряжении имеется одна базовая витрина:
                          «A_basic_shop_Karateev_VG»
и три итоговой витрины:
                          «A_final_1_showcasev_VG»
                          «A_final_2_showcasev_VG»
                          «A_final_3_showcasev_VG»''')
