from abc import ABC
from typing import Mapping

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.prompts import HumanMessagePromptTemplate
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.type.basic import Markdown
from metadata.ingestion.ometa.ometa_api import OpenMetadata


class TableDescriptionInjester(ABC):
    table_description_message = ("Дано название и список колонок таблицы. Сгенерируй короткое описание этой таблицы "
                                 "основываясь на названии колонок и названии таблицы. Описание НЕ должно содержать "
                                 "перечень колонок.")

    columns_description_message = ("Дано название и список колонок SQL таблицы. Сгенерируй описание для каждой колонки "
                                   "основываясь на названии. "
                                   "Выведи результаты в виде таблицы: Название колонки, Описание.")

    table_information_message_template = ("Название таблицы: {table_name}"
                                          "Колонки: {list_of_columns}")

    sign_of_automatic_description = "🤖"

    def __init__(self, openmetadata: OpenMetadata, llm: BaseChatModel):
        self.__llm = llm
        self.__openmetadata = openmetadata

    def _parse_column_description_from_llm_response(self, response: str) -> Mapping[str, str]:
        result = {}
        rows = response.split('\n')
        for row in rows:
            parts = [part.strip() for part in row.split("|") if len(part.strip()) > 0]

            if len(parts) < 2:
                continue

            col_name = parts[0].lower()
            col_description = parts[1].strip(".")

            result[col_name] = col_description

        return result

    def _create_table_description_prompt(self, table: Table) -> HumanMessage:
        message = (HumanMessagePromptTemplate
                   .from_template(self.table_information_message_template)
                   .format(
                        table_name=table.name.__root__,
                        list_of_columns=",".join([column.name.__root__ for column in table.columns])
                    )
        )

        return message

    def _acquire_description_for_table(self, table: Table) -> str:
        response = self.__llm(
            messages=[
                SystemMessage(content=self.table_description_message),
                self._create_table_description_prompt(table)
            ],
            temperature=0.5
        )

        return response.content

    def _acquire_description_for_columns(self, table: Table) -> Mapping[str, str]:
        response = self.__llm(
            messages=[
                SystemMessage(content=self.columns_description_message),
                self._create_table_description_prompt(table)
            ],
            temperature=0.7,
        )

        return self._parse_column_description_from_llm_response(response.content)

    def update_entity_description(self, table: Table):
        updated_table = table.copy(deep=True)

        description_of_columns = self._acquire_description_for_columns(table)
        for column in updated_table.columns:
            lookup_column_name = column.name.__root__.lower()

            if lookup_column_name in description_of_columns.keys():
                column.description = Markdown(
                    __root__=description_of_columns[lookup_column_name] + self.sign_of_automatic_description
                )

        updated_table.description = Markdown(
            __root__=self._acquire_description_for_table(table) + self.sign_of_automatic_description
        )

        self.__openmetadata.patch(entity=Table, source=table, destination=updated_table)
