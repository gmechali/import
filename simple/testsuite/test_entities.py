# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Entities (importType: entities): each CSV row becomes a typed node.

Every test imports case.csv under the shared test provenance as rows of type
Author keyed by author_id, unless it says otherwise (see entities_config()
in lib/builders.py).
"""

import pytest
from testsuite.lib import builders as b
from testsuite.lib.builders import Case
from testsuite.lib.builders import cases
from testsuite.lib.builders import known_bug


def test_rows_become_typed_nodes_keyed_by_the_id_column():
  result = b.run_csv(
      b.csv_file(["author1", "Jane Doe", "country/USA"],
                 ["author2", "Joe Smith", "country/CAN"],
                 header=["author_id", "author_name", "author_country"]),
      b.entities_config())
  b.assert_records(result.nodes, [
      b.entity("author1",
               author_id="author1",
               author_name="Jane Doe",
               author_country="country/USA"),
      b.entity("author2",
               author_id="author2",
               author_name="Joe Smith",
               author_country="country/CAN"),
  ])


def test_without_an_id_column_ids_are_generated():
  result = b.run_csv(
      b.csv_file(["Jane Doe", "country/USA"], ["Joe Smith", "country/CAN"],
                 header=["author_name", "author_country"]),
      b.entities_config(id_column=None))
  b.assert_records(result.nodes, [
      b.entity("Author_0", author_name="Jane Doe",
               author_country="country/USA"),
      b.entity(
          "Author_1", author_name="Joe Smith", author_country="country/CAN"),
  ])


def test_no_class_node_is_written_for_the_row_type():
  """Rows reference their type; the Class node itself must come from MCF."""
  result = b.run_csv(
      b.csv_file(["author1", "Jane Doe"], header=["author_id", "author_name"]),
      b.entities_config())
  b.assert_records(result.of_type("Class"), [])


def test_class_node_for_the_row_type_is_defined_in_mcf():
  result = b.run(
      {
          "case.csv":
              b.csv_file(["author1", "Jane Doe"],
                         header=["author_id", "author_name"]),
          "schema.mcf":
              b.mcf_node("dcid:Author",
                         typeOf="dcs:Class",
                         name='"Published Author"',
                         description='"A person who wrote an article"'),
      }, b.entities_config())
  b.assert_records(result.of_type("Class"), [
      b.record("Author",
               "Class",
               name="Published Author",
               description="A person who wrote an article"),
  ])


def test_reference_columns_hold_one_reference_per_listed_value():
  """In entities imports, columnsToResolve (or referenceColumns) marks reference columns."""
  result = b.run_csv(
      b.csv_file(["article1", "author1"], ["article2", "author1,author2"],
                 ["article3", "author2, author3"],
                 header=["article_id", "article_author"]),
      b.entities_config("Article",
                        "article_id",
                        columnsToResolve=["article_author"]))
  b.assert_records(result.of_type("Article"), [
      b.entity("article1",
               "Article",
               article_id="article1",
               article_author=b.ref("author1")),
      b.entity("article2",
               "Article",
               article_id="article2",
               article_author=[b.ref("author1"),
                               b.ref("author2")]),
      b.entity("article3",
               "Article",
               article_id="article3",
               article_author=[b.ref("author2"),
                               b.ref("author3")]),
  ])


def test_reference_columns_field_marks_reference_columns():
  """ReferenceColumns is accepted as the clearer name for columnsToResolve."""
  result = b.run_csv(
      b.csv_file(["article1", "author1"], ["article2", "author1,author2"],
                 header=["article_id", "article_author"]),
      b.entities_config("Article",
                        "article_id",
                        referenceColumns=["article_author"]))
  b.assert_records(result.of_type("Article"), [
      b.entity("article1",
               "Article",
               article_id="article1",
               article_author=b.ref("author1")),
      b.entity("article2",
               "Article",
               article_id="article2",
               article_author=[b.ref("author1"),
                               b.ref("author2")]),
  ])


VALUES = [
    Case("integer", given="12", expect=12),
    Case("decimal", given="3.5", expect=3.5),
    Case("text", given="Jane Doe", expect="Jane Doe"),
]


@cases(*VALUES)
def test_value(given, expect):
  result = b.run_csv(
      b.csv_file(["author1", given], header=["author_id", "value"]),
      b.entities_config())
  b.assert_records(result.of_type("Author"),
                   [b.entity("author1", author_id="author1", value=expect)])


# Modes whose entities output must match: threaded, and production
# (multiprocessing), where each file is imported by its own worker process.
_MODES = ("threaded", "production")

# A column's values are written under its header when that is a valid dcid,
# with spaces and dashes turned into underscores.
PREDICATES = [
    Case("valid dcid is kept", given="papers", expect="papers"),
    Case("space becomes underscore", given="my col", expect="my_col"),
    Case("dash becomes underscore", given="h-index", expect="h_index"),
]


@cases(*PREDICATES)
@pytest.mark.parametrize("mode", _MODES)
def test_column_header_is_the_predicate(given, expect, mode):
  result = b.run_csv(b.csv_file(["author1", "x"], header=["author_id", given]),
                     b.entities_config(),
                     mode=mode)
  b.assert_records(result.of_type("Author"),
                   [b.entity("author1", author_id="author1", **{expect: "x"})])


@pytest.mark.parametrize("mode", _MODES)
def test_mapped_column_uses_its_mapping(mode):
  result = b.run_csv(
      b.csv_file(["author1", "Jane Doe"], header=["author_id", "Nom complet"]),
      b.entities_config(columnMappings={"dcid:name": "Nom complet"}),
      mode=mode)
  b.assert_records(result.of_type("Author"),
                   [b.entity("author1", author_id="author1", name="Jane Doe")])


@pytest.mark.parametrize("mode", _MODES)
def test_id_column_header_is_sanitized_like_any_other(mode):
  result = b.run_csv(b.csv_file(["author1", "Jane Doe"],
                                header=["author id", "author_name"]),
                     b.entities_config(id_column="author id"),
                     mode=mode)
  b.assert_records(
      result.of_type("Author"),
      [b.entity("author1", author_id="author1", author_name="Jane Doe")])


@pytest.mark.parametrize("mode", [
    "threaded",
    pytest.param(
        "production",
        marks=known_bug(reason="in production, worker processes upload their"
                        " JSON-LD as they finish, so a failed run leaves the"
                        " output of the files that succeeded behind")),
])
def test_every_invalid_column_header_is_reported_at_once(mode):
  result = b.run_csv(b.csv_file(["author1", "x", "y"],
                                header=["Nº", "Catégorie", "Größe"]),
                     b.entities_config(id_column="Nº"),
                     mode=mode)
  b.assert_fails(
      result, "ValueError",
      "has column headers that are not valid property DCIDs: ['Nº',"
      " 'Catégorie', 'Größe']. Map each of them to a property DCID in"
      " 'columnMappings'")


@pytest.mark.parametrize("mode", _MODES)
def test_no_property_nodes_are_written_for_columns(mode):
  result = b.run_csv(b.csv_file(["author1", "Jane Doe"],
                                header=["author_id", "author_name"]),
                     b.entities_config(),
                     mode=mode)
  b.assert_records(result.of_type("Property"), [])


@pytest.mark.parametrize("mode", _MODES)
def test_property_node_for_a_column_is_defined_in_mcf(mode):
  result = b.run(
      {
          "case.csv":
              b.csv_file(["author1", "Jane Doe"],
                         header=["author_id", "author_name"]),
          "schema.mcf":
              b.mcf_node("dcid:author_name",
                         typeOf="dcs:Property",
                         name='"Author name"',
                         description='"Full name of the author"'),
      },
      b.entities_config(),
      mode=mode)
  b.assert_records(result.of_type("Property"), [
      b.record("author_name",
               "Property",
               name="Author name",
               description="Full name of the author"),
  ])
