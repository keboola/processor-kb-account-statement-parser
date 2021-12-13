'''
Template Component main class.

'''
import csv
import hashlib
import logging
from configparser import ParsingError
from dataclasses import asdict
from pathlib import Path

from keboola.component.base import ComponentBase
from keboola.component.dao import FileDefinition, TableDefinition
from keboola.component.exceptions import UserException

from kb_parser import parser as statement_parser
from kb_parser.parser import StatementRow, StatementMetadata, ParserError


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()

        # init table definitions
        self.statements_table: TableDefinition
        self.statement_metadata_table: TableDefinition

    def _init_tables(self):
        statement_columns = ['pk', 'statement_metadata_pk', 'row_nr']
        statement_columns.extend(list(StatementRow.__annotations__.keys()))

        statement_metadata_columns = ['pk']
        statement_metadata_columns.extend(list(StatementMetadata.__annotations__.keys()))

        self.statements_table = self.create_out_table_definition('statements.csv', incremental=True,
                                                                 columns=statement_columns,
                                                                 is_sliced=True,
                                                                 primary_key=['pk'])
        self.statement_metadata_table = self.create_out_table_definition('statements_metadata.csv', incremental=True,
                                                                         columns=statement_metadata_columns,
                                                                         is_sliced=True,
                                                                         primary_key=['pk', 'statement_pk'])

    def run(self):
        '''
        Main execution code
        '''

        self._init_tables()

        input_files = self.get_input_files_definitions(only_latest_files=False)

        pdf_files = [f for f in input_files if f.full_path.endswith('.pdf')]
        logging.info(f"{len(pdf_files)} PDF files found on the input.")

        try:
            for file in pdf_files:
                logging.info(f"Parsing file {file.name}")
                self._parse_to_csv(file)

        except ParserError as e:
            raise UserException(e) from e
        except Exception:
            raise

        # write manifest
        if pdf_files:
            self.write_manifest(self.statements_table)
            self.write_manifest(self.statement_metadata_table)

        logging.info("Parsing finished successfully!")

    def _parse_to_csv(self, pdf_file: FileDefinition):
        """
        Parse PDF statements and store as Sliced csv files.
        Args:
            pdf_file: FileDefinition

        Returns:

        """
        data_path = Path(f"{self.statements_table.full_path}/{pdf_file.name}.csv")
        data_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path = Path(f"{self.statement_metadata_table.full_path}/{pdf_file.name}.csv")
        metadata_path.parent.mkdir(parents=True, exist_ok=True)

        with open(data_path, 'w+', encoding='utf-8') as statement_out, \
                open(metadata_path, 'w+', encoding='utf-8') as metadata_out:

            data_writer = csv.DictWriter(statement_out, fieldnames=self.statements_table.columns)
            metadata_writer = csv.DictWriter(metadata_out, fieldnames=self.statement_metadata_table.columns)

            metadata_pkey = None
            idx = 0
            for data, metadata in statement_parser.parse_full_statement(pdf_file.full_path):
                dict_row = asdict(data)

                if not metadata_pkey:
                    metadata_pkey = self._build_statement_metadata_pk(metadata)

                dict_row['pk'] = self._build_statement_row_pk(idx, data, metadata_pkey)
                dict_row['statement_metadata_pk'] = metadata_pkey
                dict_row['row_nr'] = idx

                data_writer.writerow(dict_row)

                idx += 1

            # write metadata
            if metadata:
                metadata_row = asdict(metadata)
                metadata_row['pk'] = metadata_pkey
                metadata_writer.writerow(metadata_row)

    @staticmethod
    def _build_statement_row_pk(idx: int, data: StatementRow, metadata_pkey: str):
        composed_key = [idx, data.transaction_date, metadata_pkey]
        key_str = '|'.join([str(k) for k in composed_key])
        return hashlib.md5(key_str.encode()).hexdigest()

    @staticmethod
    def _build_statement_metadata_pk(metadata: StatementMetadata):
        composed_key = [metadata.statement_date, metadata.account_number, metadata.statement_number,
                        metadata.statement_type, metadata.currency]
        key_str = '|'.join([str(k) for k in composed_key])
        return hashlib.md5(key_str.encode()).hexdigest()


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
