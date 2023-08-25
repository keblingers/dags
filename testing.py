from sqlalchemy import create_engine
import pandas as pd
from pymongo import MongoClient

def invoices_mongo():
    a = MongoClient('mongodb://root:Qwerty123@kebsas02:27017')
    b = a.keblingers
    c = b.stock_ticker
    # d = [
    #     #{'$match': {'data.invoicenumber': {'$in':['INVCAKRA0709','I/PI-MKT-20','INVABINAYA1109','61/9/SA/INV/2020']}}},
    #     #{'$match': {'data.invoicenumber': {'$in':['INVCAKRA0709','I/PI-MKT-20','INVABINAYA1109','61/9/SA/INV/2020']}}},
    #     #{"$unwind":{'path':'$data', 'preserveNullAndEmptyArrays': True}},
    #     {'$project': {
    #         '_id': '$_id',
    #         'uploadedAt' : "$uploadedAt",
    #         'suppliers_id' : "$companyId",
    #         'supplier_user_id' : "$uploadBy",
    #         'status' : "$status",
    #         'anchors_name':'$data.name',
    #         'invoice_number': '$data.invoicenumber',
    #         'due_date' : '$data.duedate',
    #         'data_gross' : '$data.gross',
    #         'acceptance_status' : '$acceptanceStatus'
    #     }}
    # ]

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    result = list(c.find())
    df = pd.DataFrame(result)
    #df['kolom_baru'] = 'default'
    print(df)