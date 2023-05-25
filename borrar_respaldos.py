from google.cloud import storage
from datetime import datetime, timedelta
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


# URL de API de bencinas
BUCKET = ''

# Conexión con API de Bigquery
client = storage.Client()

dos_semanas = datetime.now() - timedelta(weeks=2)

def erase_files(rqst):
    bucket = client.get_bucket(BUCKET)
    for file in bucket.list_blobs():
        file_name = file.name.replace('.csv', '')
        file_date = datetime.strptime(file_name, '%Y-%m-%d')
        if file_date < dos_semanas:
            print(f'Se elimina archivo {file.name}')
            file.delete()
    return '{"status":"200", "data": "Función ejecutada"}'
