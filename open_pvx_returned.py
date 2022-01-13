import pandas as pd
import sys

from io import StringIO

from base64 import b64encode

from pymyreader import pymyreader

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

       # Return all Magento Orders that are cancelled/refunded so James can manually refund them

       magento_order_sql = """SELECT o.increment_id as magento_order_id, oi.sku, oi.qty_ordered as item_qty_refunded
                              from mage_sales_order o
                              left join mage_sales_order_item oi on oi.order_id = o.entity_id where DATE(o.created_at) >= '{}' and DATE(o.created_at) <= '{}'  and o.status in ('canceled','closed') """.format(start_date, end_date)

       magento_orders = pymyreader(magento_order_sql)

       magento_orders['item_qty_refunded'] = magento_orders['item_qty_refunded'].astype(int)

       PVX_Open_Returned = PVX_Sales.merge(magento_orders, left_on = ['Sales order no.','Item code'], right_on = ['magento_order_id','sku'])

       PVX_Open_Returned.to_csv('Open PVX + Refunded Magento {} - {}.csv'.format(start_date,end_date),index = False)


open_pvx_returned(sys.argv[1],sys.argv[2],sys.argv[3])
