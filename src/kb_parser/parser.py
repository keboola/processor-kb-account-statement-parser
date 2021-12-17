import json
import logging
import math
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Iterator, Tuple, Union, Callable

import tabula

PANDAS_OPTIONS = {'dtype': str}

JAVA_OPTIONS = '-XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap -Xmx200m'


class ParserError(Exception):
    pass


def _get_templates_directory():
    return Path(__file__).parent.joinpath('templates')


class HeaderTemplatePaths(Enum):
    account_type = Path(_get_templates_directory(), 'account_type_header.tabula-template.json').as_posix()
    report_metadata = Path(_get_templates_directory(), 'report_metadata_header.tabula-template.json').as_posix()
    account_entity = Path(_get_templates_directory(), 'account_entity_header.tabula-template.json').as_posix()
    total_balance = Path(_get_templates_directory(), 'total_balance_header.tabula-template.json').as_posix()


class DataTemplatePaths(Enum):
    last_page = Path(_get_templates_directory(), 'last_page.tabula-template.json').as_posix()


@dataclass
class StatementMetadata:
    account_number: str = ''
    statement_type: str = ''
    iban: str = ''
    account_type: str = ''
    currency: str = ''
    statement_date: str = ''
    statement_number: str = ''
    account_entity: str = ''
    start_balance: float = 0
    end_balance: float = 0


@dataclass
class StatementRow:
    accounting_date: str = ''
    transaction_date: str = ''
    transaction_description: str = ''
    transaction_identification: str = ''
    account_name__card_type: str = ''
    account_number__merchant: str = ''
    vs: str = ''
    ks: str = ''
    ss: str = ''
    transaction_type: str = ''
    amount: float = 0


def _get_table_value_strict(data_row: dict, column_name_key: str, value_key: str, expected_column_name: str,
                            errors_buffer: List[str]):
    """
    Helper function to retrieve and validate Table column value with strict name.
    Args:
        data_row:
        column_name_key:
        value_key:
        expected_column_name:
        errors_buffer:

    Returns:

    """
    value = None
    if data_row.get(column_name_key) == expected_column_name:
        value = data_row[value_key]
    else:
        errors_buffer.append(f"Missing '{expected_column_name}' section.")
    return value


def _validate_row_structure(row: dict, column_count: int, section_name: str) -> List[str]:
    """
    Validates row_data structure (column count) and returns list of column headers (keys)
    Args:
        row:
        column_count:
        section_name:

    Returns: List of column keys

    """
    if len(row) != column_count:
        raise ParserError(f"{section_name} has different amount of columns {len(row)}"
                          f" than expected {column_count}!")

    return list(row.keys())


def _convert_to_numeric(number_str: Union[float, str]):
    formatted = str(number_str).replace(',', '.')
    pattern = re.compile(r'\s+')
    formatted = re.sub(pattern, '', formatted)
    return float(formatted)


def _convert_na_to_empty(string: str):
    return str(string).replace('nan', '')


def _load_single_page_section_from_template(file_path: str, section_name: str,
                                            template_path: Union[HeaderTemplatePaths, DataTemplatePaths],
                                            page_nr='1', stream=False):
    """
    Load data from sepcified templated located on single page
    Args:
        file_path: path to pdf file
        section_name:
        template_path: path to template
        page_nr: number of the page
        stream:

    Returns:

    """
    _id, path = tempfile.mkstemp()
    with open(template_path.value, 'r') as template_in, open(path, 'w') as tmp_out:
        template_json = json.load(template_in)
        template_json[0]['page'] = page_nr
        json.dump(template_json, tmp_out)

    try:
        df = tabula.read_pdf_with_template(file_path, path,
                                           pandas_options=PANDAS_OPTIONS,
                                           java_options=JAVA_OPTIONS,
                                           stream=stream,
                                           pages=page_nr)[0]
        return df.to_dict('records')
    except KeyError:
        raise ParserError(f'Statement {Path(file_path).name} does not contain the {section_name} '
                          f'on expected position!')


def _parse_account_type_metadata(file_path: str, statement_metadata: StatementMetadata):
    """
    Parses account type section and updates StatementMetadata values in-place
    Args:
        file_path:
        statement_metadata (StatementMetadata): Metadata container to be updated

    Returns:

    """
    dict_rows = _load_single_page_section_from_template(file_path, 'Account type section',
                                                        HeaderTemplatePaths.account_type)

    # validate initial
    if len(dict_rows) < 4:
        raise ParserError("Header Account type section is missing some rows!")

    if len(dict_rows[0]) != 2:
        raise ParserError(f"Header Account type section has different amount of columns {len(dict_rows[0])}"
                          f" than expected!")
    parse_errors = []

    column_key = list(dict_rows[0].keys())[0]
    vypis_type_key = list(dict_rows[0].keys())[1]
    statement_metadata.statement_type = vypis_type_key

    # account number
    statement_metadata.account_number = _get_table_value_strict(dict_rows[0], column_key, vypis_type_key,
                                                                'k účtu:', errors_buffer=parse_errors)
    # IBAN
    statement_metadata.iban = _get_table_value_strict(dict_rows[1], column_key, vypis_type_key,
                                                      'IBAN:', errors_buffer=parse_errors)
    # start type
    statement_metadata.account_type = _get_table_value_strict(dict_rows[2], column_key, vypis_type_key,
                                                              'typ:', errors_buffer=parse_errors)
    # currency:
    statement_metadata.currency = _get_table_value_strict(dict_rows[3], column_key, vypis_type_key,
                                                          'měna:', errors_buffer=parse_errors)

    if parse_errors:
        raise ParserError(f"Header Account type section parsing failed with errors: {'; '.join(parse_errors)}")

    return statement_metadata


def _parse_report_metadata(file_path: str, statement_metadata: StatementMetadata):
    """
    Parses report metadata section and updates StatementMetadata values in-place
    Args:
        file_path:
        statement_metadata (StatementMetadata): Metadata container to be updated

    Returns:

    """
    section_name = 'Report Metadata section'
    dict_rows = _load_single_page_section_from_template(file_path, section_name, HeaderTemplatePaths.report_metadata)

    # validate initial
    if len(dict_rows) < 3:
        raise ParserError(f"{section_name} section is missing some rows!")

    if len(dict_rows[0]) != 2:
        raise ParserError(f"{section_name} has different amount of columns {len(dict_rows[0])}"
                          f" than expected!")
    parse_errors = []

    first_column_key = list(dict_rows[0].keys())[0]
    second_column_key = list(dict_rows[0].keys())[1]

    # Datum is first row_data
    statement_metadata.statement_date = second_column_key
    # cislo
    statement_metadata.statement_number = _get_table_value_strict(dict_rows[0], first_column_key, second_column_key,
                                                                  'Číslo výpisu:', errors_buffer=parse_errors)

    if parse_errors:
        raise ParserError(f"{section_name} parsing failed with errors: {'; '.join(parse_errors)}")

    return statement_metadata


def _parse_balance_section_metadata(file_path: str, statement_metadata: StatementMetadata):
    """
    Parses balance section and updates StatementMetadata values in-place
    Args:
        file_path:
        statement_metadata (StatementMetadata): Metadata container to be updated

    Returns:

    """
    section_name = 'Report Balance section'
    dict_rows = _load_single_page_section_from_template(file_path, section_name, HeaderTemplatePaths.total_balance)

    # validate initial
    if len(dict_rows) < 1:
        raise ParserError(f"{section_name} section is missing some rows!")

    if len(dict_rows[0]) != 2:
        raise ParserError(f"{section_name} has different amount of columns {len(dict_rows[0])}"
                          f" than expected!")

    parse_errors = []

    column_name_key = list(dict_rows[0].keys())[0]
    second_column_key = list(dict_rows[0].keys())[1]

    # pocatecni is first row_data key
    statement_metadata.start_balance = _convert_to_numeric(second_column_key)

    # koncovy
    end_balance_str = _get_table_value_strict(dict_rows[0], column_name_key, second_column_key,
                                              'Konečný zůstatek', errors_buffer=parse_errors)
    if end_balance_str:
        statement_metadata.end_balance = _convert_to_numeric(end_balance_str)

    if parse_errors:
        raise ParserError(f"{section_name} parsing failed with errors: {'; '.join(parse_errors)}")


def _parse_entity_section(file_path: str, statement_metadata: StatementMetadata):
    """
    Parses entity metadata section and updates StatementMetadata values in-place
    Args:
        file_path:
        statement_metadata (StatementMetadata): Metadata container to be updated

    Returns:

    """
    section_name = 'Account Entity section'
    dict_rows = _load_single_page_section_from_template(file_path, section_name, HeaderTemplatePaths.account_entity)

    # validate initial
    if len(dict_rows) < 1:
        raise ParserError(f"{section_name} section is missing some rows!")

    if len(dict_rows[0]) > 2:
        raise ParserError(f"{section_name} has different amount of columns {len(dict_rows[0])}"
                          f" than expected!")

    column_name_key = list(dict_rows[0].keys())[0]
    second_column_key = ''

    # sometimes the columns are split
    if len(dict_rows[0]) == 2:
        second_column_key = list(dict_rows[0].keys())[1]

    entity_rows = [f'{column_name_key} {second_column_key}\n']
    for row in dict_rows:
        row = [str(value) for value in row.values() if str(value) != 'nan']
        entity_rows.append(' '.join(row))
    statement_metadata.account_entity = '\n'.join(entity_rows)

    return statement_metadata


def parse_statement_metadata(file_path: str) -> StatementMetadata:
    """
    Parse statement metadata present on the first page.
    Args:
        file_path: path to the PDF statement.

    Returns: StatementMetadata - container with metadata values

    """
    statement_metadata = StatementMetadata()
    _parse_account_type_metadata(file_path, statement_metadata)
    _parse_report_metadata(file_path, statement_metadata)
    _parse_balance_section_metadata(file_path, statement_metadata)
    _parse_entity_section(file_path, statement_metadata)

    return statement_metadata


def _get_full_statement_rows(file_path: str) -> Iterator[Iterator[dict]]:
    for df in tabula.read_pdf(file_path,
                              stream=True,
                              pandas_options=PANDAS_OPTIONS,
                              java_options=JAVA_OPTIONS,
                              pages='all'):
        yield (row for row in df.to_dict('records'))


def _get_last_page_statement_rows(file_path: str) -> Iterator[Iterator[dict]]:
    pages_nr = len(tabula.read_pdf(file_path, guess=False, pages='all',
                                   java_options=JAVA_OPTIONS))

    # Sometimes the last page is not parsed properly so use predefined template
    last_page_records = _load_single_page_section_from_template(file_path, 'last_page', DataTemplatePaths.last_page,
                                                                str(pages_nr), stream=True)
    yield (row for row in last_page_records)


def _validate_statement_header_first_row(column_names: List[str], column_number=5):
    first_row_keys_4 = [['Datum Popis transakce'], ['Název protiúčtu / Číslo a typ karty', 'Unnamed: 0'],
                        ['VS', 'Název protiúčtu / Číslo a typ karty'], ['Připsáno', 'VS']]
    first_row_keys_5 = [['Datum', 'Datum Popis transakce'], ['Popis transakce', 'Unnamed: 0'],
                        ['Název protiúčtu / Číslo a typ karty'], ['VS'], ['Připsáno']]
    if column_number == 5:
        first_row_keys = first_row_keys_5
    elif column_number == 4:
        first_row_keys = first_row_keys_4
    else:
        raise ValueError()

    errors = []
    for idx, names in enumerate(first_row_keys):
        if column_names[idx] not in names:
            errors.append(f"Column '{names}' is expected on {idx}. position. '{column_names[idx]}' found instead"),

    if errors:
        raise ParserError(f"Failed to parse the statement transactions header. Found errors: "
                          f"\nf{'; '.join(errors)}")


def _skip_statement_data_header(statement_page: Iterator[dict]) -> Tuple[Callable, bool]:
    """
    Iterates and validates the statement page header.
    Returns function to modify the records to expected structure
    This can happen when there's some other type of table detected.

    Args:
        statement_page: (Iterator[dict]) Iterator of page rows.

    Returns: merge_columns:bool, convert_function:Callable

    """
    first_row = next(statement_page)
    dict_keys = list(first_row.keys())

    # Some reports have recap page at the end. Skip that from parsing
    if list(first_row.keys())[0] == 'Rekapitulace transakcí na účtu':
        return _pass, True

    if len(first_row) == 4:
        convert_method = _pass
    elif len(first_row) == 5 and dict_keys[0] == 'Datum Popis transakce' and dict_keys[4] in ['Unnamed: 0']:
        convert_method = _drop_last_column
    elif len(first_row) == 5 and dict_keys[0] == 'Datum Popis transakce' and dict_keys[1] in ['Unnamed: 0']:
        convert_method = _merge_second_two_columns
    elif len(first_row) == 5 and dict_keys[0] == 'Datum':
        convert_method = _merge_first_two_columns
    else:
        raise ParserError(f"Statement Page Header has different amount of columns [{len(first_row)}] than expected")

    first_row = convert_method(first_row)

    dict_keys = list(first_row.keys())

    if dict_keys[0] == 'POČÁTEČNÍ ZŮSTATEK':
        first_row_header = list(first_row.values())
    else:
        first_row_header = dict_keys

    _validate_statement_header_first_row(first_row_header, column_number=len(first_row))

    is_last_header_row = False
    skipped_rows = 1
    while not is_last_header_row:
        row = next(statement_page)
        skipped_rows += 1
        if skipped_rows > 4:
            raise ParserError("The Statement Page Header has more rows than expected!")
        if list(row.values())[0] == 'transakce':
            break

    return convert_method, False


def _split_date_from_text(text: str):
    date_part = text.split(' ')[0]
    text = text.split(' ')[1]
    # validate
    datetime.strptime(date_part, "%d.%m.%Y")
    return date_part, text


def _parse_first_statement_row_part(row_data: List[str], statement_row_data: StatementRow):
    first_header = row_data[0]
    try:

        date_part, description = _split_date_from_text(first_header)
        account_name = row_data[1]

        statement_row_data.accounting_date = date_part
        statement_row_data.transaction_description = description
        statement_row_data.account_name__card_type = account_name
        statement_row_data.vs = _convert_na_to_empty(row_data[2])
        amount = _convert_to_numeric(row_data[3])
        statement_row_data.amount = amount

        statement_row_data.transaction_type = 'debit' if amount < 0 else 'credit'

    except Exception as e:
        raise ParserError(f"The first statement row_data part has invalid structure: {first_header}") from e

    return date_part, description


def _parse_second_statement_row_part(row_data: List[str], statement_row_data: StatementRow):
    # try to split date
    try:
        date, identification_text = _split_date_from_text(row_data[0])
    except Exception:
        date = ''
        identification_text = row_data[0]

    statement_row_data.transaction_identification = identification_text
    statement_row_data.transaction_date = date

    statement_row_data.account_number__merchant = row_data[1]
    statement_row_data.ks = _convert_na_to_empty(row_data[2])


def _parse_third_statement_row_part(row_data: List[str], statement_row_data: StatementRow):
    identification_text = _convert_na_to_empty(row_data[0])
    statement_row_data.transaction_identification += f"\n{identification_text}"

    statement_row_data.ss = _convert_na_to_empty(row_data[2])


def _is_end_of_statement_data(row: dict):
    row_values = list(row.values())
    values_string = ' '.join([str(v) for v in row_values])
    end_of_page = 'Pokračování na další straně' in values_string

    return len(row_values) == 0 or row_values[0] == 'KONEČNÝ ZŮSTATEK' or end_of_page


def _is_date_text_split(text):
    contains_date = True
    try:
        _split_date_from_text(text)
    except Exception:
        contains_date = False
    return contains_date


# # Record modifying functions

def _pass(row: dict):
    return row


def _merge_first_two_columns(row: dict):
    # TODO: Consider reversing the process => unmerging two columns as it seems only first page has this issue
    new_dict = {}
    keys = list(row.keys())
    values = list(row.values())
    first_two_merged = [_convert_na_to_empty(v) for v in values[:2]]
    new_dict[' '.join(keys[:2])] = ' '.join(first_two_merged)

    for idx, val in enumerate(values[2:], start=2):
        new_dict[keys[idx]] = val

    return new_dict


def _merge_second_two_columns(row: dict):
    new_dict = {}
    keys = list(row.keys())
    values = list(row.values())

    for idx, val in enumerate(values):

        if idx == 2:
            new_dict[keys[2 - 1]] = _convert_na_to_empty(new_dict[keys[2 - 1]]) + _convert_na_to_empty(val)
        else:
            new_dict[keys[idx]] = val

    return new_dict


def _drop_last_column(row: dict):
    row.pop(list(row.keys())[-1:][0])
    return row


def _get_next_transformed(page_iterator: Iterator[dict], convert_method: Callable = _pass):
    row = next(page_iterator, {})
    return convert_method(row)


def _parse_next_statement_row(page_iterator: Iterator[dict],
                              convert_method: Callable = _pass,
                              first_row_part=None) -> Tuple[StatementRow, dict]:
    """
    Builds statement data row from multiple row parts.

    Args:
        page_iterator: Iterator of page rows
        convert_method: Flag whether to modify the output structure to match the expected.
                             Sometimes the first two columns are merged, if not (5 columns) this flag should be set
                             to true.
        first_row_part: Next statement row returned from the method

    Returns: (Tuple[StatementRow, dict]) Statement row data and next row in the iterator if present.

    """
    statement_row_data = StatementRow()
    # Sometimes the first two are merged, merge to 4 which is expected
    merge_cols = convert_method

    # if starting from the beginning
    if not first_row_part:
        first_row_part = _get_next_transformed(page_iterator, merge_cols)

    _validate_row_structure(first_row_part, 4, 'Statement Page Data')

    row_part_data = list(first_row_part.values())
    _parse_first_statement_row_part(row_part_data, statement_row_data)

    # There should always be second part
    row_part = _get_next_transformed(page_iterator, merge_cols)
    row_part_data = list(row_part.values())
    _parse_second_statement_row_part(row_part_data, statement_row_data)

    # Third part
    row_part = _get_next_transformed(page_iterator, merge_cols)
    row_part_data = list(row_part.values())
    is_end = _is_end_of_statement_data(row_part) or _is_date_text_split(row_part_data[0])
    if not is_end:
        _parse_third_statement_row_part(row_part_data, statement_row_data)

    # Remaining identification parts
    if not is_end:
        row_part = _get_next_transformed(page_iterator, merge_cols)
        row_part_data = list(row_part.values())
        is_end = (_is_end_of_statement_data(row_part) or _is_date_text_split(row_part_data[0]))

        while not is_end:
            text = _convert_na_to_empty(row_part_data[1])
            statement_row_data.transaction_identification += f"\n{text}"
            row_part = _get_next_transformed(page_iterator, merge_cols)
            row_part_data = list(row_part.values())
            is_end = _is_end_of_statement_data(row_part) or _is_date_text_split(row_part_data[0])

    # if complete end do not return next page
    if _is_end_of_statement_data(row_part):
        next_page = None
    else:
        next_page = row_part

    return statement_row_data, next_page


def _iterate_through_rows(pages_iterator, processing_metadata: dict):
    for page_iterator in pages_iterator:
        processing_metadata['pages_processed'] += 1
        logging.info(f"Processing page #{processing_metadata['pages_processed']}")
        convert_method, skip = _skip_statement_data_header(page_iterator)

        # end reached
        if skip:
            break

        has_next = True
        next_page = None
        while has_next:
            data, next_page = _parse_next_statement_row(page_iterator, convert_method, next_page)
            if not next_page:
                has_next = False

            # validation
            if data.amount < 0:
                processing_metadata['debit_total'] += data.amount
            else:
                processing_metadata['credit_total'] += data.amount

            yield data


def parse_full_statement(file_path: str) -> Tuple[StatementRow, StatementMetadata]:
    statement_metadata = parse_statement_metadata(file_path)
    processing_metadata = dict(
        pages_processed=0,
        # for validation
        debit_total=0,
        credit_total=0)

    iterator = _iterate_through_rows(_get_full_statement_rows(file_path),
                                     processing_metadata=processing_metadata)
    for i in iterator:
        yield i, statement_metadata

    total_sum = processing_metadata['debit_total'] + processing_metadata['credit_total']
    total_sum_check = statement_metadata.end_balance - statement_metadata.start_balance
    if math.ceil(total_sum) != math.ceil(total_sum_check):
        # Possibly the last page parsing failes, retry with template
        logging.warning('The end sum does not match, trying to reprocess last page from template.')
        processing_metadata['pages_processed'] -= 1
        iterator = _iterate_through_rows(_get_last_page_statement_rows(file_path),
                                         processing_metadata=processing_metadata)
        for i in iterator:
            yield i, statement_metadata

    total_sum = processing_metadata['debit_total'] + processing_metadata['credit_total']
    total_sum_check = statement_metadata.end_balance - statement_metadata.start_balance
    if math.ceil(total_sum) != math.ceil(total_sum_check):
        raise ParserError(
            f"Parsed result ended with inconsistent data. The transaction sum from totals {total_sum_check} "
            f"is not equal to sum of individual transactions {total_sum}")
