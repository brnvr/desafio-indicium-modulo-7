import pandas as pd
import os
from extract import extract_from_table

def count_items_ordered_to_rio(orders_csv_filename):
    df_orders = pd.read_csv(orders_csv_filename, encoding='ISO-8859-1')
    order_details = extract_from_table("OrderDetail")
    df_order_details = pd.DataFrame(order_details[1:], columns=order_details[0])
    df_orders_to_rio = df_orders[df_orders['ShipCity'] == 'Rio de Janeiro']
    df_orders_merged = pd.merge(df_order_details, df_orders_to_rio, left_on='OrderId', right_on='Id')

    return df_orders_merged['Quantity'].sum()