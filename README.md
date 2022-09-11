# Python Flask app on Azure App Service Web

This is a minimal sample app that demonstrates how to run a Python Flask application on Azure App Service Web.

# ENV EXAMPLE
CONNECTION_STRING=
AccountName=
AccountKey=
CONTAINER_NAME=
ACCOUNT_NAME=

POSTGRES_HOST=
POSTGRES_DB_NAME=
POSTGRES_USER=
POSTGRES_PASSWORD=

# Api URLS
## Ver datos guardados en azure postgres DataBase service

+ /inventario
+ /productos
+ /sucursales
+ /clientes

## Descragar csv de azure data lake

+ /guardar/csv/inventario
+ /guardar/csv/productos
+ /guardar/csv/sucursales
+ /guardar/csv/clientes

## Insertar datos en azure postgres DataBase service

+ /inventario/insertar
+ /productos/insertar
+ /sucursales/insertar
+ /clientes/insertar