import pandas as pd
import numpy as np

import sys

from pymyreader import pymyreader
from gcs_connect import bucket

def fraudulent_sales_checker(start_date, end_date, password, **kwargs):
       
       sql = """
       select o.increment_id as ID,
       CASE when o.store_id = 1 THEN 'Main Website'
              when o.store_id = 2 THEN 'Irish Website'
       when o.store_id = 3 THEN 'US Website'
       END as `Purchase Point`, o.created_at as `Purchase Date`, CONCAT(ad.firstname,' ', ad.lastname) as `Bill-to-Name`,CONCAT(ad2.firstname,' ', ad2.lastname) as `Ship-to-Name`,o.base_grand_total as `Grand Total (Base)`, o.grand_total as `Grand Total (Purchased)`, o.status as Status,CONCAT(ad.street,', ',ad.city,', ', ad.region,', ', ad.postcode) as `Billing Address`, CONCAT(ad2.street,', ', ad2.city,', ', ad2.region, ', ',ad2.postcode) as `Shipping Address`, o.customer_email as `Customer Email`,fo.status as `Futura Status`, fo.branch_id as `Futura Branch`,o.pvx_status as `Pvx Status`,  CONCAT(oi.sku,',', oi.name) as Product, oi.qty_ordered
       from mage_sales_order o
       left join mage_sales_order_item oi on o.entity_id = oi.order_id
       left join mage_tco_futura_order fo on o.entity_id = fo.magento_id
       left join mage_catalog_product_entity pe on oi.sku = pe.sku
       left join mage_sales_order_address ad on o.entity_id = ad.parent_id and ad.address_type = 'billing'
       left join mage_sales_order_address ad2 on o.entity_id = ad2.parent_id and ad2.address_type = 'shipping'
       where DATE(o.created_at) >= '{}'  and DATE(o.created_at) <= '{}' and oi.store_id in (1,2,3)  and o.grand_total > 500""".format(start_date, end_date)

       sales = pymyreader(sql)

       #fill nulls for groupby

       sales[['ID', 'Purchase Point', 'Purchase Date', 'Bill-to-Name', 'Ship-to-Name',
       'Grand Total (Base)', 'Grand Total (Purchased)', 'Status',
       'Billing Address', 'Shipping Address', 'Customer Email',
       'Futura Status', 'Futura Branch', 'Pvx Status']] = sales[['ID', 'Purchase Point', 'Purchase Date', 'Bill-to-Name', 'Ship-to-Name',
       'Grand Total (Base)', 'Grand Total (Purchased)', 'Status',
       'Billing Address', 'Shipping Address', 'Customer Email',
       'Futura Status', 'Futura Branch', 'Pvx Status']].fillna('/')

       sales[['Grand Total (Base)', 'Grand Total (Purchased)','qty_ordered']] = sales[['Grand Total (Base)', 'Grand Total (Purchased)','qty_ordered']].astype(float)

       joiner = sales.groupby(['ID', 'Purchase Point', 'Purchase Date', 'Bill-to-Name', 'Ship-to-Name',
              'Grand Total (Base)', 'Grand Total (Purchased)', 'Status',
              'Billing Address', 'Shipping Address', 'Customer Email',
              'Futura Status', 'Futura Branch', 'Pvx Status']).filter(lambda x: x['qty_ordered'].sum() > 1)

       joiner['Products'] = joiner['Product'] + ' ' + joiner['qty_ordered'].astype(int).astype(str) + ' units sold'

       final = joiner.groupby(['ID', 'Purchase Point', 'Purchase Date', 'Bill-to-Name', 'Ship-to-Name',
       'Grand Total (Base)', 'Grand Total (Purchased)', 'Status',
       'Billing Address', 'Shipping Address', 'Customer Email',
       'Futura Status', 'Futura Branch', 'Pvx Status'])['Products'].apply('::'.join).reset_index()

       from openpyxl import load_workbook

       book = load_workbook('Fraudulent_Activity_Template.xlsx')

       writer = pd.ExcelWriter('Fraudulent Activity {}.xlsx'.format(start_date), engine='openpyxl')
       writer.book = book
       writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
       final.to_excel(writer, "Fraudulent Activity by Postcode", startrow = 6, index = False,header = False)
       writer.save()

       import pyminizip as zip

       zip_name = 'Fraudulent_Activity_{}.zip'.format(start_date)

       zip.compress('Fraudulent Activity {}.xlsx'.format(start_date), None, 'Fraudulent_Activity_{}.zip'.format(start_date), password, 1)

       return zip_name

zip_file = fraudulent_sales_checker(sys.argv[1],sys.argv[2],sys.argv[3])

bucket(zip_file, 'cs-fraud')