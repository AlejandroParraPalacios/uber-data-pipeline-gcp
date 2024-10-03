from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_big_query(output, **kwargs) -> None:
    """
    Exporta todas las tablas generadas (fact_table y dimensiones) a un almacén de BigQuery.
    """

    # Ruta de configuración
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Inicializar BigQuery con la configuración
    bigquery_client = BigQuery.with_config(ConfigFileLoader(config_path, config_profile))

    # Definir los IDs de las tablas en BigQuery
    table_ids = {
        'fact_table': 'project-1-437422.uber_dataset.fact_table',
        'datetime_dim': 'project-1-437422.uber_dataset.datetime_dim',
        'passenger_count_dim': 'project-1-437422.uber_dataset.passenger_count_dim',
        'trip_distance_dim': 'project-1-437422.uber_dataset.trip_distance_dim',
        'rate_code_dim': 'project-1-437422.uber_dataset.rate_code_dim',
        'pickup_location_dim': 'project-1-437422.uber_dataset.pickup_location_dim',
        'dropoff_location_dim': 'project-1-437422.uber_dataset.dropoff_location_dim',
        'payment_type_dim': 'project-1-437422.uber_dataset.payment_type_dim'
    }

    # Cargar cada tabla a BigQuery
    for table_name, table_id in table_ids.items():
        if table_name in output:
            print(f"Cargando {table_name} a BigQuery...")
            bigquery_client.export(
                DataFrame(output[table_name]),
                table_id,
                if_exists='replace',  # Reemplazar si la tabla ya existe
            )
            print(f"{table_name} cargado exitosamente.")
        else:
            print(f"Advertencia: {table_name} no está en la salida y no se ha cargado.")