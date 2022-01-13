import datetime as dt
from isoweek import Week
import sys


def PVX_Mag_Order_Disrepancy(pvx_password, slack_secret,date_set,**kwargs):

    import pandas as pd
    from io import BytesIO, StringIO
    from base64 import b64encode

    from PVXreader import PvxClient
    from pymyreader import pymyreader
    from slack_message import slack

    date = dt.datetime.strptime(date_set, '%Y-%m-%d')

    PVX_Client = PvxClient(client_id='ob2752', username='HarryB', password=b64encode(bytes(pvx_password, encoding ='utf-8')))

    # Set the PVX date to the date before to account for acceptable sync lag

    PVX_date = date - dt.timedelta(days=2)

    # Reformat date for entry in PVX SOAP API
    Datetime = "([Requested delivery date] > DateTime({},{:02d},{:02d},00,00,00))".format(PVX_date.year, PVX_date.month, PVX_date.day)

    PVX_Orders = PVX_Client.get_report(report_name='Sales orders by status',
                                       columns="[Sales order no.],[Status],[Requested delivery date]", filters=Datetime)

    # Read CSV output to memory to create DataFrame: PVX_Orders
    Input = StringIO(PVX_Orders['Detail'])
    PVX_Orders = pd.read_csv(Input)

    # Split on delimiter ('-') for multiple part orders
    PVX_Orders['Sales order no.'] = PVX_Orders['Sales order no.'].str.split('-').str[0]
    
    # Dictate script to access Magento Orders on mySQL

    script = """SELECT created_at, increment_id,status
    FROM oliverbonas_prod_magento.mage_sales_order 
    WHERE created_at > '%s' AND status = 'processing';"""
    sql_date = date.strftime('%Y-%m-%d')

    Magento_Orders = pymyreader(sql=script % (sql_date))

    # Test whether Magento orders are in PVX and return missing

    Missed = Magento_Orders[~Magento_Orders['increment_id'].astype(str).isin(PVX_Orders['Sales order no.'].astype(str))]

    Missed_not_now = Missed[Missed['created_at'] < dt.datetime.now() - dt.timedelta(hours = 1)]

    # saving a data frame to a buffer (same as with a regular file):
    s_buf = BytesIO()
    Missed_not_now.to_csv(s_buf, index=False)
    s_buf.seek(0)


    # create slack connection and then post created csv and topline stat
    connection = slack(slack_secret)

    connection.slack_message_send(channel='G01A3GFJ726',message='There are {} missing orders in Magento that do not exist in PVX from '.format(len(Missed_not_now['increment_id'])) + sql_date + ' to now')
    connection.slack_upload(channels= 'G01A3GFJ726', title='Missing Orders in Magento from (' + sql_date + ') to now.csv', df=s_buf, filename = 'Missing Orders in Magento from (' + sql_date + ') to now.csv')

# 'DQJK9J50V'
PVX_Mag_Order_Disrepancy(sys.argv[1],sys.argv[2],sys.argv[3])
