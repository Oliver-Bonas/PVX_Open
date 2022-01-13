import pandas as pd
import sys

from io import StringIO

from base64 import b64encode
import json

from pymyreader import pymyreader
from gcs_connect import bucket
from PVXreader import PvxClient

def open_pvx_returned(start_date, end_date, pvx_password, **kwargs):

       # convert strings to datetime objects

       start_datetime = pd.to_datetime(start_date, format = '%Y-%m-%d')
       end_datetime = pd.to_datetime(end_date, format = '%Y-%m-%d')

       # Return all PVX Orders yet to be shipped

       PVX_Client = PvxClient(client_id='ob2752', username='HarryB', password=b64encode(bytes(pvx_password, encoding ='utf-8')))

       Datetime_Filter = "([Requested delivery date] >= DateTime({},{:02d},{:02d},00,00,00)) AND ([Requested delivery date] <= DateTime({},{:02d},{:02d},00,00,00))"\
              .format(start_datetime.year,start_datetime.month,start_datetime.day, end_datetime.year,end_datetime.month,end_datetime.day)

       PVX_Orders = PVX_Client.get_report(report_name = 'Outstanding sales orders', columns = "[Sales order no.],[Requested delivery date],[Item_Name],[Item code]",filters = Datetime_Filter )
       
       Input = StringIO(PVX_Orders['Detail'])

       PVX_Sales = pd.read_csv(Input)

       PVX_Sales['Requested delivery date'] = pd.to_datetime(PVX_Sales['Requested delivery date'], format = "'%d/%m/%Y %H:%M:%S'")

       PVX_Sales['Requested date'] = PVX_Sales['Requested delivery date'].dt.date

       PVX_Sales.rename(columns = {'Sales order no.':'magento_order_id','Item code':'sku'}, inplace = True)

       # Return all Magento Orders that are cancelled/refunded so James can manually refund them

       magento_order_sql = """SELECT o.increment_id as magento_order_id, oi.sku, oi.qty_ordered as item_qty_refunded
                              from mage_sales_order o
                              left join mage_sales_order_item oi on oi.order_id = o.entity_id where DATE(o.created_at) >= '{}' and DATE(o.created_at) <= '{}'  and o.status in ('canceled','closed') """.format(start_date, end_date)

       magento_orders = pymyreader(magento_order_sql)

       csv_name = 'Open PVX + Refunded Magento {} - {}.csv'.format(start_date,end_date)

       if magento_orders.empty:

              PVX_Open_Returned = pd.DataFrame(columns = ['magento_order_id','sku','Requested delivery date','Item Name','item_qty_refunded'])

       else:
       
              print(start_date, end_date)

              print(magento_orders)

              magento_orders['item_qty_refunded'] = magento_orders['item_qty_refunded'].astype(int)

              magento_orders['sku'] = magento_orders['sku'].astype(str)
              magento_orders['magento_order_id'] = magento_orders['magento_order_id'].astype(str)

              PVX_Sales['sku'] = PVX_Sales['sku'].astype(str)
              PVX_Sales['magento_order_id'] = PVX_Sales['magento_order_id'].astype(str)

              

              PVX_Open_Returned = PVX_Sales.merge(magento_orders, on = ['magento_order_id','sku']).reset_index()

       
       PVX_Open_Returned.to_csv(csv_name, index = False)


       xcom_return = {"csv_name":csv_name, "output_length":len(PVX_Open_Returned)}

       return xcom_return

open_pvx_xcom = open_pvx_returned(sys.argv[1],sys.argv[2],sys.argv[3])

# write to gcs bucket 'open_pvx_returns'

bucket(open_pvx_xcom['csv_name'], 'open_pvx_returns')

# write xcom output to return.json

with open("/airflow/xcom/return.json", "w") as file:
       json.dump(xcom_return, file)
