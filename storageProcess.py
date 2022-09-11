import csv
import psycopg2
from dotenv import load_dotenv
import os
from os import walk

load_dotenv()

local_path = "./data"
if not os.path.exists(local_path):
    os.mkdir(local_path)

host = os.getenv('POSTGRES_HOST')
dbname = os.getenv('POSTGRES_DB_NAME')
user = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')
sslmode = "require"

conn_string = f"host={host} user={user} dbname={dbname} password={password} sslmode={sslmode}"

def get_connection_string():
    return conn_string

def csv_data_to_list(path):
    listFiles = []
    dataList = []

    for (dirpath, dirnames, filenames) in walk(path):
        listFiles.extend(filenames)
        break

    for files in listFiles:
        filePath: str = path
        with open(filePath) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=';')
            line_count = 0
            for row in csv_reader:
                dataList.append(f'\t{row[0]};{row[1]};{row[2]}')
                line_count += 1

    return dataList


def sql_sentence_executor(str_insert_sentence):
    try:
        postgres_connection = psycopg2.connect(conn_string)
        cursor = postgres_connection.cursor()

        cursor.execute(str_insert_sentence)
        extracted_query_result = []

        cursor.execute(str_insert_sentence)

        if 'SELECT' or 'select' in str_insert_sentence:
            rows = cursor.fetchall()
            for row in rows:
                extracted_query_result.append(row)
        postgres_connection.commit()

    except postgres_connection.Error as e:
        try:
            print(f"POSTGRESQL Error {e.args[0]}: {e.args[1]}")
            return None
        except IndexError:
            print(f"POSTGRESQL Error: {str(e)}")
            return None
        except TypeError as e:
            print(e)
            return None
        except ValueError as e:
            print(e)
            return None
    finally:
        cursor.close()
        postgres_connection.close()
    return extracted_query_result


def csv_reformat_to_sql(data: str):
    data_list = data.split(";")
    refactor_data = []

    for data in data_list:
        if not data.isnumeric():
            data = ("'" + data + "'")
        refactor_data.append(data)

    return refactor_data


def data_orchestrator(csv_path: str):
    inventory_file: str

    list_target = csv_data_to_list(csv_path)

    if 'Local' in csv_path:
        for inventory_csv in list_target:
            csv_refactor = csv_reformat_to_sql(inventory_csv)
            insert_sentence = f'INSERT INTO public.sucursal("idSucural", ubicacion, descripcion) VALUES ({csv_refactor[0]}, {csv_refactor[1]}, {csv_refactor[2]});'
            sql_sentence_executor(insert_sentence)

    elif 'User' in csv_path:
        for inventory_csv in list_target:
            csv_refactor = csv_reformat_to_sql(inventory_csv)
            insert_sentence = f'INSERT INTO public.cliente("idCliente", nombre, "tipoUsuario") VALUES ({csv_refactor[0]}, {csv_refactor[1]}, {csv_refactor[2]});'
            sql_sentence_executor(insert_sentence)

    elif 'Product' in csv_path:
        for inventory_csv in list_target:
            csv_refactor = csv_reformat_to_sql(inventory_csv)
            insert_sentence = f'INSERT INTO public.producto("idProducto", nombre, descripcion) VALUES ({csv_refactor[0]}, {csv_refactor[1]}, {csv_refactor[2]});'
            sql_sentence_executor(insert_sentence)
    else:
        for inventory_csv in list_target:
            csv_refactor = csv_reformat_to_sql(inventory_csv)
            insert_sentence = f'INSERT INTO public.inventario("idInventario", "FechaInventario", "GLN_Cliente", "GLN_sucursal", "Gtin_Producto", "Inventario_Final", "PrecioUnidad")VALUES ({csv_refactor[0]}, {csv_refactor[1]}, {csv_refactor[2]}, {csv_refactor[3]}, {csv_refactor[4]}, {csv_refactor[5]}, {csv_refactor[6]});'
            sql_sentence_executor(insert_sentence)