# Copyright 2024 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import re

import pandas as pd
from stats.data import FileValidationError
from stats.data import RowEntity
from stats.data import strip_namespace
from stats.data import Triple
from stats.data import ValidationErrorType
from stats.graph_writer import GraphWriter
from stats.importer import Importer
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from stats.util import has_namespace_prefix
from util.filesystem import File

# Pattern to check if a string conforms to that of a valid DCID.
_DCID_PATTERN = r"^(?:[A-Za-z0-9_/]+:)?[A-Za-z0-9_/]+$"


def _parse_object_ids(v_str: str) -> str | list[str]:
  """Splits comma-separated entity references and strips namespace prefixes."""
  if "," in v_str:
    return [strip_namespace(x.strip()) for x in v_str.split(",")]
  return strip_namespace(v_str)


def _sanitize_dcid(raw: str) -> str | None:
  """Returns `raw` (or `raw` with spaces/dashes replaced by `_`) if it is a valid DCID."""
  if re.fullmatch(_DCID_PATTERN, raw):
    return raw
  candidate = re.sub(r"[ -]", "_", raw)
  if re.fullmatch(_DCID_PATTERN, candidate):
    return candidate
  return None


class EntitiesImporter(Importer):
  """Imports a single entities input file.

  Key behaviors at this time:
  - All un-ignored columns will be encoded as property triples. A column's predicate is its `columnMappings` entry, else its header as a DCID (spaces and dashes become `_`). Headers that are not valid DCIDs must be mapped.
  - If an id column was configured, it will be used as the entity dcid. Else a new dcid will be generated for each entity.
  - Columns specified as entity columns will be encoded as object_id in the triples tables. Others will be encoded as object_value.
    + Currently this importer does not resolve any entities and all entities are assumed to be pre-resolved into dcids.
    """

  def __init__(self, input_file: File, graph_writer: GraphWriter,
               reporter: FileImportReporter, nodes: Nodes) -> None:
    self.input_file = input_file
    self.graph_writer = graph_writer
    self.reporter = reporter
    self.nodes = nodes
    self.config = nodes.config
    self.ignore_columns = self.config.ignore_columns(self.input_file)
    self.provenance_id = ""

    self.row_entity_type = self.config.row_entity_type(self.input_file)
    assert self.row_entity_type, f"Row entity type must be specified: {self.input_file.full_path()}"

    self.id_column = self.config.id_column(self.input_file)
    # Reassigned when renaming columns.
    self.entity_columns = set(self.config.entity_columns(self.input_file))

    # Initialize reverse column mappings from config
    column_mappings = self.get_column_mappings()
    self.has_column_mappings = bool(column_mappings)
    self.reverse_mappings = self.get_reverse_column_mappings()

    self.df = pd.DataFrame()

  def do_import(self) -> None:
    self.reporter.report_started()
    try:
      errors = self.validate_headers()
      if errors:
        raise ValueError("\n".join(e.error_message for e in errors))
      self.provenance_id = self.nodes.provenance(self.input_file).id
      self._read_csv()
      self._drop_ignored_columns()
      self._rename_columns()
      self._write_row_entity_triples()
      self.reporter.report_success()
    except Exception as e:
      self.reporter.report_failure(str(e))
      raise e

  def validate_headers(self) -> list[FileValidationError]:
    errors = []
    if not self.has_column_mappings:
      return []

    try:
      with self.input_file.open_stream() as stream:
        header_df = pd.read_csv(stream, nrows=0)
      actual_columns = set(header_df.columns)
    except Exception as e:
      return [
          FileValidationError(
              file=self.input_file.path,
              error_type=ValidationErrorType.GENERIC_ERROR,
              error_message=
              f"Failed to read CSV headers for '{self.input_file.path}': {str(e)}"
          )
      ]

    mapped_columns = set(self.reverse_mappings.keys())
    ignored_columns = set(self.ignore_columns)
    id_col = {self.id_column} if self.id_column else set()
    all_allowed_columns = mapped_columns | ignored_columns | id_col
    unmapped_columns = actual_columns - all_allowed_columns
    if unmapped_columns:
      errors.append(
          FileValidationError(
              file=self.input_file.path,
              error_type=ValidationErrorType.UNMAPPED_COLUMNS,
              problem_columns=sorted(list(unmapped_columns)),
              error_message=
              f"The CSV file '{self.input_file.path}' contains unmapped columns: {sorted(list(unmapped_columns))}. Please map them in 'columnMappings' or list them in 'ignoreColumns' in config.json."
          ))

    return errors

  def _read_csv(self) -> None:
    # Read CSVs with the following behaviors:
    # - Strip leading whitespaces
    # - Treat comma as a thousands separator
    self.df = pd.read_csv(self.input_file.read_string_io(),
                          skipinitialspace=True,
                          thousands=",")
    logging.info("Read %s rows.", self.df.index.size)
    # Convert to best possible dtypes (i.e. keep ints as ints even when some values are NaN)
    self.df = self.df.convert_dtypes()

  def _drop_ignored_columns(self):
    if self.ignore_columns:
      self.df.drop(columns=self.ignore_columns, axis=1, inplace=True)

  def _is_entity_reference_column(self, col: str) -> bool:
    return col in self.entity_columns

  def _column_predicate(self, col: str) -> str | None:
    """Returns the predicate a column's values are written under, if any.

    That is the column's `columnMappings` entry if it has one, else the header
    itself when it is (or, with spaces/dashes as `_`, becomes) a valid DCID.
    """
    if col in self.reverse_mappings:
      return self.reverse_mappings[col]
    return _sanitize_dcid(col)

  def _rename_columns(self) -> None:
    """Renames every column to its predicate.

    Raises:
      ValueError: naming every column that has no valid predicate.
    """
    renamed = {col: self._column_predicate(col) for col in self.df.columns}
    invalid = [col for col, predicate in renamed.items() if not predicate]
    if invalid:
      raise ValueError(
          f"The CSV file '{self.input_file.path}' has column headers that are "
          f"not valid property DCIDs: {invalid}. Map each of them to a "
          "property DCID in 'columnMappings' (or list them in 'ignoreColumns') "
          "in config.json.")
    self.entity_columns = {renamed.get(col, col) for col in self.entity_columns}
    self.df = self.df.rename(columns=renamed)

  def _extract_row_properties(
      self,
      row: pd.Series) -> tuple[dict[str, str], dict[str, str | list[str]]]:
    """Splits a CSV row into literal property values and entity reference IDs."""
    prop_object_values: dict[str, str] = {}
    prop_object_ids: dict[str, str | list[str]] = {}
    for k, v in row.items():
      if pd.isna(v):
        continue
      v_str = str(v)
      if self._is_entity_reference_column(k) or has_namespace_prefix(v_str):
        prop_object_ids[k] = _parse_object_ids(v_str)
      else:
        prop_object_values[k] = v
    return prop_object_values, prop_object_ids

  def _write_row_entity_triples(self) -> None:
    # All columns have been renamed to their predicates by now, so look the id
    # column up by its predicate.
    id_column_name = ""
    if self.id_column:
      id_column_name = self._column_predicate(self.id_column)

    triples: list[Triple] = []
    entity_type = strip_namespace(self.row_entity_type)
    for index, row in self.df.iterrows():
      raw_dcid = row[
          id_column_name] if id_column_name else f"{self.row_entity_type}_{index}"
      dcid = strip_namespace(str(raw_dcid))
      prop_object_values, prop_object_ids = self._extract_row_properties(row)
      row_entity = RowEntity(dcid,
                             entity_type,
                             provenance_id=self.provenance_id,
                             prop_object_values=prop_object_values,
                             prop_object_ids=prop_object_ids)
      triples.extend(row_entity.triples())

    self.graph_writer.write_triples(triples, self.input_file)
