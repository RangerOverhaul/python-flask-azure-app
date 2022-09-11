import azureBlobFiles
import storageProcess
import os

from dotenv import load_dotenv
from flask import Flask, request, jsonify

load_dotenv()

local_path = "./data"
if not os.path.exists(local_path):
    os.mkdir(local_path)

c_string = os.getenv('CONNECTION_STRING')
c_name = os.getenv('CONTAINER_NAME')

app = Flask(__name__)

if __name__ == '__main__':
    app.run()

connection_string = storageProcess.get_connection_string()


def download_blobs(target):
    result = azureBlobFiles.download_files(c_string, c_name, target)
    return result


def path_exits(path):
    if os.path.exists(path):
        return True
    else:
        return False

@app.route('/guardar/csv/inventario', methods=['GET'])
def download_inventory_blob():
    if download_blobs('Inventory'):
        inventory_list = storageProcess.csv_data_to_list('./data', 'Inventory')
        return jsonify(inventory_list)
    else:
        return "Error no se pudo encontar el alamacenamiento"


@app.route('/guardar/csv/productos', methods=['GET'])
def download_product_blob():
    if download_blobs('Product'):
        product_list = storageProcess.csv_data_to_list('./data', 'Product')
        return jsonify(product_list)
    else:
        return "Error no se pudo encontar el alamacenamiento"


@app.route('/guardar/csv/sucursales', methods=['GET'])
def download_branches_blob():
    if download_blobs('Local'):
        branches_list = storageProcess.csv_data_to_list('./data', 'Local')
        return jsonify(branches_list)
    else:
        return "Error no se pudo encontar el alamacenamiento"


@app.route('/guardar/csv/clientes', methods=['GET'])
def download_user_blob():
    if download_blobs('User'):
        inventory_list = storageProcess.csv_data_to_list('./data', 'User')
        return jsonify(inventory_list)
    else:
        return "Error no se pudo encontar el alamacenamiento"


@app.route('/inventario', methods=['GET'])
def get_inventory():
    result = storageProcess.sql_execute_sentence("SELECT * FROM inventario")
    return result


@app.route('/clientes', methods=['GET'])
def get_clients():
    result = storageProcess.sql_execute_sentence("SELECT * FROM cliente")
    return result


@app.route('/sucursales', methods=['GET'])
def get_branches():
    result = storageProcess.sql_execute_sentence("SELECT * FROM sucursal")
    return result


@app.route('/productos', methods=['GET'])
def get_products():
    result = storageProcess.sql_execute_sentence("SELECT * FROM producto")
    return result


@app.route('/inventario/insertar', methods=['POST'])
def insert_inventory_data():
    target_path = './data/targetInventoryFile.csv'
    if path_exits(target_path):
        try:
            storageProcess.data_orchestrator(target_path)
            os.remove(target_path)
            return "Inventario cargado en base de datos"
        except:
            return "Ha ocurrido un error al intenar cargar el inventario en la base de datos"
    else:
        return "No existe el archivo targetInventoryFile.csv"


@app.route('/clientes/insertar', methods=['POST'])
def insert_client_data():
    target_path = './data/targetUserFile.csv'
    if path_exits(target_path):
        try:
            storageProcess.data_orchestrator(target_path)
            os.remove(target_path)
            return "clientes cargados en base de datos"
        except:
            return "Ha ocurrido un error al intenar cargar los clientes en la base de datos"
    else:
        return "No existe el archivo targetUserFile.csv"


@app.route('/sucursales/insertar', methods=['POST'])
def insert_branches_data():
    target_path = './data/targetLocalFile.csv'
    if path_exits(target_path):
        try:
            storageProcess.data_orchestrator(target_path)
            os.remove(target_path)
            return "Sucursales cargadas en base de datos"
        except:
            return "Ha ocurrido un error al intenar cargar las sucursales en la base de datos"
    else:
        return "No existe el archivo targetLocalFile.csv"


@app.route('/productos/insertar', methods=['POST'])
def insert_product_data():
    target_path = './data/targetProductFile.csv'
    if path_exits(target_path):
        try:
            storageProcess.data_orchestrator(target_path)
            os.remove(target_path)
            return "Productos cargados en base de datos"
        except:
            return "Ha ocurrido un error al intenar cargar los productos en la base de datos"
    else:
        return "No existe el archivo targetProductFile.csv"
