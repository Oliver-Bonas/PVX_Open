import os

def pymyreader(sql, source='Magento'):
    import pymysql
    import pandas as pd

    # Connect to the database
    if source == 'Magento':
        connection = pymysql.connect(host=os.environ.get('MAGE_HOST'),
                                     user='oliverbonas-read',
                                     password=os.environ.get('MAGE_PASS'),
                                     db='oliverbonas_prod_magento',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

    elif source == 'localhost':
        connection = pymysql.connect(host='',
                                     user='root',
                                     password='',
                                     db='oliverbonasnew_live',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
    else:
        print("Source must either be: 'Magento' or 'localhost'")
        return

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
            return pd.DataFrame(result)

    finally:
        connection.close()