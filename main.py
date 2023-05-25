from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import requests as rq
from datetime import datetime, timedelta
import time
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


# URL de API de bencinas
URL = 'https://api.cne.cl/v3/combustibles/vehicular/estaciones?token=LI6qhEbKop'

# Nombre del bucket para respaldos
BUCKET = ''

# Conexión con API de Bigquery
client = bigquery.Client()
storage_client = storage.Client()

# Reemplazar por los ID de las tablas de Bigquery
TABLA_BENCINERAS = ''
TABLA_FECHA = ''
TABLA_PRECIO_COMBUSTIBLE = ''
TABLA_UBICACION = ''

###########################################################################################

def get_data_from_api():
    '''
    Función que realiza la conexión con la API de combustibles.
    Realiza 5 intentos de conexión, arrojando un error y deteniendo
    el programa en caso de no poder conectarse.
    Retorna todos los datos en formato json.
    '''
    for i in range(5):
        error = True
        try:
            res = rq.get(URL)
        except rq.exceptions.RequestException:
            print('Error al intentar conectarse a la API, intentando nuevamente...')
            time.sleep(3)
        else:
            error = False
            break
    if error:
        raise Exception('No se puede conectar a la API')
    return res.json()['data']


############################################################################################

def check_if_backup():
    '''
    Verifica si ya existe el respaldo del día en el
    que se está ejecutando el trabajo y elimina
    dicho resplado para reemplazarlo por los datos nuevos.
    '''
    today = datetime.now().strftime('%Y-%m-%d')
    bucket = storage_client.get_bucket(BUCKET)

    for file in bucket.list_blobs():
        file_name = file.name.replace('.csv', '')
        if file_name ==  today:
            print('Respaldo ya guardado, reemplazando..')
            file.delete()


############################################################################################


def save_backup(df):
    '''
    Guardar respaldo de la respuesta de la API
    en un Bucket en formato CSV   
    '''
    # Crea un archivo CSV
    csv_data = df.to_csv(index=False)

    # Generando un nombre de archivo con la fecha
    nombre_archivo = f"{datetime.now().strftime('%Y-%m-%d')}.csv"

    # Verificando si existe respaldo en la misma fecha, y eliminándolo
    check_if_backup()

    # Guardando en el bucket el respaldo
    bucket = storage_client.bucket(BUCKET)
    blob = bucket.blob(nombre_archivo)
    blob.upload_from_string(csv_data, content_type='text/csv')


############################################################################################


def data():
    '''
    Función que realiza la limpieza y transformación de
    los datos, junto con algunas modificaciones.
    También elimina columnas que no se utilizarán, se
    cambian los tipos de datos según corresponda y se agrega
    una columna de ID que se usará más adelante.
    Retorna un DataFrame de pandas
    '''
# Solicitud a la API
    data = get_data_from_api()

    # Creación del dataframe a partir del json recibido
    df = pd.json_normalize(data)

    # Respaldando
    save_backup(df)

    # Reemplazando caracteres en los nombres de las columnas para
    # que sean aceptados por bigquery
    df.columns = df.columns.astype(str).str.replace('.', '_')
    df.columns = df.columns.astype(str).str.replace(' ', '_')

    # Formato uniforme para los datos en columnas 'razon_social' y 'direccion_calle'
    df['razon_social'] = df['razon_social'].str.capitalize()
    df['direccion_calle'] = df['direccion_calle'].str.capitalize()

    # Eliminando palabras redundantes de los nombres de las columnas
    for i in ['precios_', 'metodos_de_', 'servicios_', 'ubicacion_']:
        df.columns = df.columns.astype(str).str.replace(i, '')

    # Cambiando el tipo de dato a INTEGER
    for i in ['gasolina_93', 'gasolina_95', 'gasolina_97', 'petroleo_diesel', 'glp_vehicular', 'gnc']:
        df[i] = df[i].apply(pd.to_numeric).round(0).astype('Int64')

    # Renombrando algunas columnas
    df.rename(columns={'fecha_hora_actualizacion': 'fecha_actualizacion', 
                        'id': 'id_bencinera'}, inplace=True)

    # En vez de considerar la última actualización como fecha, su usará la
    # fecha actual para registrar cuando se consultaron los datos, 
    # y se asume que ese es el valor actual de la bencina
    fecha = datetime.now().strftime('%Y-%m-%d')
    df['fecha_actualizacion'] = fecha
    df['fecha_actualizacion'] = pd.to_datetime(df['fecha_actualizacion'])

    # Eliminando columnas que no se usarán
    df.drop(['id_comuna', 'id_region', 'distribuidor_logo', 'distribuidor_logo_horizontal_svg'], axis=1, inplace=True)
    
    #Creando un ID correlativo de las ubicaciones
    df['id_ubicacion'] = df.index + 1
    
    print('Proceso de limpieza de datos terminado\n\n')
    return df

#######################################################################

def insert_fecha():
    '''
    La función verifica si la fecha de hoy ya se encuentra
    almacenada en la base de datos. De no ser así, crea
    una nueva fila en la tabla fecha y pasa el ID de esa
    fecha para usarlo en la tabla de hecho.
    En caso de ya estar guardada la fecha de hoy, recupera
    el ID de la fecha para usarla en la creación de la tabla
    de hecho
    '''
    print('Insertando fecha\n\n')
    
    # Consulta a la tabla fecha
    q = f"SELECT * FROM `{TABLA_FECHA}`"
    # Fecha de hoy
    fecha = datetime.now().strftime('%Y-%m-%d')
    res = client.query(q)
    n_rows = res.result().total_rows
    res = res.to_dataframe()
    # Se verifica si la tabla tiene registros
    if n_rows > 0:
        # Se verifica si algun registro corresponde a la fecha de hoy
        if res.iloc[-1].fecha.strftime('%Y-%m-%d') == fecha:
            # Si la fecha de hoy está, se recupera el id
            return res.iloc[-1].id_fecha
        else:
            # Si la fecha no está, se crea un nuevo registro
            next_id_fecha = int(res['id_fecha'].max() + 1)
            to_insert = {'id_fecha': next_id_fecha, 'fecha': fecha}
            client.insert_rows_json(TABLA_FECHA, [to_insert])
            return next_id_fecha
    # Si no había ningun registro, se crea la primera fila
    to_insert = {'id_fecha': 1, 'fecha': fecha}
    client.insert_rows_json(TABLA_FECHA, [to_insert])
    return 1

########################################################################

def check_alredy_executed():
    '''
    Función que verifica si ya se ejecutó el trabajo hoy.
    En caso de haberse ejecutado, se borran los registros
    de hoy y se reemplazan por nuevos
    '''
    print('Verificando si ya se ejecutó el trabajo hoy\n\n')
    fecha = datetime.now()
    fecha = fecha.strftime('%Y-%m-%d')
    query = f"SELECT id_fecha from `{TABLA_FECHA}` WHERE fecha = '{fecha}'"
    res = client.query(query)
    n_rows = res.result().total_rows
    if n_rows > 0:
        print('Está la fecha de hoy en tabla fecha\n\n')
        last_id_fecha = list(res)[0][0]
        q = f"SELECT id_precio_combustible from `{TABLA_PRECIO_COMBUSTIBLE}` WHERE id_fecha = {last_id_fecha}"
        res_comb = client.query(q)
        n_rows_comb = res_comb.result().total_rows
        if n_rows_comb > 0:
            print('Hay registros de hoy en tabla precios\n\n')
            print(f'Eliminando registros con id_fecha {last_id_fecha}\n\n')
            dlt_stm = f"DELETE from `{TABLA_PRECIO_COMBUSTIBLE}` WHERE id_fecha = {last_id_fecha}"
            query_resp = client.query(dlt_stm)
    else:
        print('Trabajo no ejecutado hoy\n\n')
    
##########################################################################

def set_id(df):
    '''
    Función que crea la columna del ID de los precios
    de los combustibles. El objetivo es que sea un correlativo,
    por lo que esta función se encarga de ver cual fue el último
    registro y continuar desde ahí.
    '''
    print('Creando columna id_precio_combustible\n\n')
    q = F"SELECT * FROM `{TABLA_PRECIO_COMBUSTIBLE}`"
    res = client.query(q)
    n_rows = res.result().total_rows
    if n_rows > 0:
        last_id_q = f"SELECT id_precio_combustible FROM `{TABLA_PRECIO_COMBUSTIBLE}` ORDER BY id_precio_combustible DESC LIMIT 1"
        res = client.query(last_id_q)
        last_id = list(res)[0][0] + 1
        df['id_precio_combustible'] = df.index + last_id
    else:
        df['id_precio_combustible'] = df.index + 1
    return df

###########################################################################

def main_table(df):
    '''
    Se crea un nuevo DataFrame con los mismos campos de la tabla
    de hecho en Bigquery. Se hace un loop por el DataFrame inicial
    y se crea una fila nueva por cada tipo de combustible con su
    respectivo precio. Si una bencinera no distribuye cierto tipo,
    su valor será Null, por lo que no se tomará en consideración para
    el nuevo DataFrame.
    '''
    print('Creando tabla de precios de combustibles\n\n')
    comb_tipos = ['gasolina_93', 'gasolina_95', 'gasolina_97', 'petroleo_diesel', 'glp_vehicular', 'gnc']
    df_2 = df.copy()
    fut_df = []
    for index, row in df_2.iterrows():
        for tipo in comb_tipos:
            precio = row[tipo]
            if precio is pd.NA:
                continue
            new_row = [row.id_bencinera, row.id_ubicacion, tipo, precio]
            fut_df.append(new_row)
    df_hecho = pd.DataFrame(data=fut_df, columns=['id_bencinera', 'id_ubicacion', 'tipo_combustible', 'precio'])
    df_hecho['id_fecha'] = insert_fecha()
    df_hecho = set_id(df_hecho)
    job_config = bigquery.LoadJobConfig()
    job = client.load_table_from_dataframe(
        df_hecho, TABLA_PRECIO_COMBUSTIBLE, job_config=job_config)
    job.result()

##############################################################################

def is_empty(id_tabla):
    '''
    Verificar si una tabla tiene o no registros.
    '''
    q = f"SELECT * FROM `{id_tabla}`"
    res = client.query(q)
    n_rows = res.result().total_rows
    if n_rows > 0:
        print(f'Tabla {id_tabla} tiene registros\n\n')
        False
    else:
        print(f'Tabla {id_tabla} NO tiene registros\n\n')
        return True
    
##############################################################################

def build_updt_stm(table_id, fields_dict, where_field, where_value, where_field_string):
    '''
    Función que crea el Update statement a partir de los datos
    que se pasen como parámetro.
    '''
    update_stm = f'UPDATE `{table_id}` SET '
    dict_len = len(fields_dict)
    counter = 0
    for key, value in fields_dict.items():
        counter += 1
        if type(value) == str:
            update_stm += f"{key} = '{value}'"
        else:
            update_stm += f"{key} = {value}"
        if counter < dict_len:
            update_stm += ', ' 
        else:
            update_stm += ' '
    if where_field_string:
        update_stm += f"WHERE {where_field} = '{where_value}'"
    else:
        update_stm += f"WHERE {where_field} = {where_value}"
    print(update_stm)
    client.query(update_stm)

#############################################################################

def updated_bencin(df_bencin):
    '''
    Función que actualiza las bencineras en caso de que ya existan
    registros en la tabla "bencineras" y agrega nuevas bencineras
    en caso de encontrar nuevos registros
    '''
    benc_table = client.query(f'SELECT * FROM {TABLA_BENCINERAS}')
    benc_df = benc_table.to_dataframe()
    rows_to_insert = []
    for index, row in df_bencin.iterrows():
        row_to_compare = benc_df.loc[benc_df['id_bencinera'] == row.id_bencinera, :]
        if len(row_to_compare.index) == 1:
            to_update = {}
            for col in row_to_compare:
                if row[col] != row_to_compare[col].values[0]:
                    to_update[col] = row[col]
            if len(to_update) > 0:
                print('Fila actualizada')
                print(to_update)
                build_updt_stm(TABLA_BENCINERAS, to_update, 'id_bencinera', row.id_bencinera, True)
        else:
            print('Nueva Bencinera!\n\n')
            rows_to_insert.append(row.to_dict())
    if len(rows_to_insert) > 0:
        print(f'Se agregan {len(rows_to_insert)} filas a la tabla {TABLA_BENCINERAS}\n\n')
        err = client.insert_rows_json(TABLA_BENCINERAS, rows_to_insert)
        print(err)
    else:
        print(f'No hay nuevos registros para la tabla {TABLA_BENCINERAS}\n\n')

######################################################################################

def bencinas(df):
    
    df_copy = df.copy()
    df_bencin = df_copy[['id_bencinera', 'razon_social', 'distribuidor_nombre',
                          'distribuidor_logo_svg', 'tienda', 'farmacia', 'mantencion',
                          'autoservicio', 'pago_efectivo', 'pago_cheque', 
                           'pago_tarjetas_bancarias', 'pago_tarjetas_grandes_tiendas']]
    if is_empty(TABLA_BENCINERAS): 
        print('Creando tabla de bencineras\n\n')
        job_config = bigquery.LoadJobConfig()
        job = client.load_table_from_dataframe(
            df_bencin, TABLA_BENCINERAS, job_config=job_config)
        job.result()
    else:
        print('Actualizando tabla de bencineras\n\n')
        updated_bencin(df_bencin)

################################################################################

def update_ubic(df_ubic):
    ubic_table = client.query(f'SELECT * FROM {TABLA_UBICACION}')
    ubic_df = ubic_table.to_dataframe()
    rows_to_insert = []
    for index, row in df_ubic.iterrows():
        row_to_compare = ubic_df.loc[ubic_df['id_bencinera'] == row.id_bencinera, :]
        if len(row_to_compare.index) == 1:
            to_update = {}
            for col in row_to_compare:
                if col == 'id_ubicacion':
                    continue
                elif row[col] != row_to_compare[col].values[0]:
                    to_update[col] = row[col]
            if len(to_update) > 0:
                print(to_update)
                build_updt_stm(TABLA_UBICACION, to_update, 'id_bencinera', row.id_bencinera, True)
        else:
            print('Nueva Bencinera!')
            new_row = row.to_dict()
            last_id_q = f"SELECT id_ubicacion FROM `{TABLA_UBICACION}` ORDER BY id_ubicacion DESC LIMIT 1"
            res = client.query(last_id_q)
            last_id = list(res)[0][0] + 1
            print('Siguiente ID:', last_id)
            new_row['id_ubicacion'] = last_id
            rows_to_insert.append(new_row)
    if len(rows_to_insert) > 0:
        print(f'Se agregan {len(rows_to_insert)} filas a la tabla {TABLA_UBICACION}\n\n')
        client.insert_rows_json(TABLA_UBICACION, rows_to_insert)
    else:
        print(f'No hay nuevos registros para la tabla {TABLA_UBICACION}\n\n')

###########################################################################################

def ubicacion(df):
    print('Creando tabla de ubicaciones\n\n')
    df_ubicacion = df[['id_ubicacion', 'nombre_comuna', 'nombre_region',
                    'latitud', 'longitud', 'id_bencinera']].copy()
    df_ubicacion['direccion'] = df['direccion_calle'] + ', ' + df['direccion_numero']
    if is_empty(TABLA_UBICACION):
        print('Creando tabla de ubicaciones\n\n')
        job_config = bigquery.LoadJobConfig()
        job = client.load_table_from_dataframe(
            df_ubicacion, TABLA_UBICACION, job_config=job_config)
        job.result()
    else:
        print('Actualizando tabla de ubicaciones\n\n')
        update_ubic(df_ubicacion)
        
#############################################################################


def main_function(rqst):
    print('Empezando proceso de extracción y limpieza de datos\n\n')
    df = data()
    check_alredy_executed()
    main_table(df)
    bencinas(df)
    ubicacion(df)
    return '{"status":"200", "data": "Función ejecutada"}'
