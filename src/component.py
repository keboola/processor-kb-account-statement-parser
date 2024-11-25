'''
Template Component main class.

'''
import csv
import hashlib
import logging
import os
import re
import shutil
from dataclasses import asdict
from itertools import groupby
from pathlib import Path
from typing import List

from PyPDF2 import PdfFileMerger, PdfFileReader
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
                                                                 schema=statement_columns,
                                                                 is_sliced=True,
                                                                 primary_key=['pk'])
        self.statement_metadata_table = self.create_out_table_definition('statements_metadata.csv', incremental=True,
                                                                         schema=statement_metadata_columns,
                                                                         is_sliced=True,
                                                                         primary_key=['pk'])

    def run(self):
        '''
        Main execution code
        '''

        self._init_tables()

        input_files = self.get_input_files_definitions(only_latest_files=False)

        pdf_files = [f for f in input_files if f.full_path.endswith('.pdf')]
        logging.info(f"{len(pdf_files)} PDF files found on the input.")

        # merge files that are split
        pdf_files = self._merge_split_files(pdf_files)

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

    def _merge_pdfs(self, paths: List[str], result_path: str):
        """
        Merges pdfs into one.
        Args:
            paths:
            result_path:

        Returns:

        """

        # Call the PdfFileMerger
        merged_object = PdfFileMerger()

        for file in paths:
            merged_object.append(PdfFileReader(file, 'rb'))

        # Write all the files into a file which is named as shown below
        merged_object.write(result_path)

    def _merge_split_files(self, pdf_files) -> List[FileDefinition]:
        """
        Merges statement files and returns File objects.
        Args:
            pdf_files:

        Returns:

        """
        r = re.compile(r'^(.*)\dz\d.pdf')
        file_paths = [f.full_path for f in pdf_files]
        files_to_merge = list(filter(r.match, file_paths))

        # remove these files from the pdf_file list
        normal_files = [f for f in pdf_files if f.full_path not in files_to_merge]

        split_files = self._group_split_files(files_to_merge)
        if len(split_files) > 0:
            logging.info(f'{len(split_files)} files split files received, merging.')

        result_files = []
        for key in split_files:
            result_path = f"{key}.pdf"
            logging.info(f"Merging {len(split_files[key])} parts into {result_path}.")
            file_definition = self._create_file_definition(name=Path(result_path).name, storage_stage='in')
            self._merge_pdfs(split_files[key], file_definition.full_path)

            # remove source files
            self._delete_files(split_files[key])
            result_files.append(file_definition)

        result_files.extend(normal_files)

        return result_files

    def _delete_files(self, paths: List[str]):
        for p in paths:
            os.unlink(p)

    def _group_split_files(self, split_files: List[str]):
        """
        Returns split files grouped by name
        Args:
            split_files:

        Returns:

        """

        def group_key(name: str):
            r = re.compile(r'^.*?(\d+_\d+_ucet_\d+)_\d+z\d+\.pdf')
            group = r.match(name).group(1)
            return group

        def sort_key(name: str):
            r = re.compile(r'^.*_(\d+z\d+)\.pdf')
            group = r.match(name).group(1)
            return group

        files_to_merge = {}
        for k, g in groupby(sorted(split_files, key=group_key), key=group_key):
            files_to_merge[k] = sorted(list(g), key=sort_key)
        return files_to_merge

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

            data_writer = csv.DictWriter(statement_out, fieldnames=self.statements_table.column_names)
            metadata_writer = csv.DictWriter(metadata_out, fieldnames=self.statement_metadata_table.column_names)

            metadata_pkey = None
            idx = 0
            for data, metadata in statement_parser.parse_full_statement(pdf_file.full_path):
                if not data:
                    continue
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

            # move in_tables untouched
            self._move_in_tables()

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

    def _move_in_tables(self):
        for t in self.get_input_tables_definitions():
            source_path = t.full_path
            t.full_path = t.full_path.replace('in', 'out')
            t.stage = 'out'
            shutil.move(source_path, t.full_path)
            self.write_manifest(t)


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
