import datetime
from airflow.decorators import task
from airflow.models.dag import dag
from airflow.models import Variable
from langchain_community.chat_models import ChatYandexGPT
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import \
    OpenMetadataConnection, AuthProvider
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from TableDescriptionInjester import TableDescriptionInjester


@task.python()
def run():
    llm = ChatYandexGPT(
        api_key=Variable.get("yandexgpt_api_key"),
        folder_id=Variable.get("yandexgpt_folder_id"),
        model_name="yandexgpt",
    )

    openmetadata = OpenMetadata(OpenMetadataConnection(
        hostPort=Variable.get('openmetadata_api_endpoint'),
        authProvider=AuthProvider.openmetadata,
        securityConfig=OpenMetadataJWTClientConfig(
            jwtToken=Variable.get('openmetadata_jwt_token')
        ),
    ))

    table_description_injester = TableDescriptionInjester(openmetadata, llm)
    all_tables = openmetadata.list_all_entities(entity=Table)
    for table in all_tables:
        table_description_injester.update_entity_description(table)


@dag(
    dag_id="open-metadata-auto-description-bot",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
)
def dag():
    run()






