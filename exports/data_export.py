import sqlite3
import pandas as pd
import os

carpeta = 'exports'  # puede ser una ruta absoluta o relativa
os.makedirs(carpeta, exist_ok=True)  # crea la carpeta si no existe


for tabla in ['orders', 'products', 'employees', 'shippers', 'customers', 'suppliers', 'dates']:
    conn = sqlite3.connect('medical_star.db')
    df = pd.read_sql_query(f'SELECT * FROM {tabla}', conn)

    ruta_csv = os.path.join(carpeta, f'{tabla}.csv')

    df.to_csv(ruta_csv, index=False)
    conn.close()

