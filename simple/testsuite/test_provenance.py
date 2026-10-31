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
Provenances and sources: how they're registered, attached and validated.

Every inputFiles entry names a provenance, which an MCF file must define with
a source. Most tests here replace the shared provenance.mcf (see
_provenance_mcf) to vary that.
"""

import json

from testsuite.lib import builders as b
from testsuite.lib.builders import Case
from testsuite.lib.builders import cases

_ONE_ROW = b.csv_file(b.obs_row())


def _provenance_mcf(with_source: bool = True,
                    **provenance_props: str) -> dict[str, str]:
  """
  A provenance.mcf to use as shared_files: the test provenance and source.

  provenance_props are extra properties of the provenance (raw MCF). With
  with_source=False, the source node is left out and the provenance links to
  no source, unless provenance_props sets one.
  """
  blocks = []
  if with_source:
    blocks.append(
        b.mcf_node(f"dcid:{b.SOURCE}",
                   typeOf="dcs:Source",
                   name='"Test Source"',
                   url='"https://source.example.org"'))
    provenance_props = {"source": f"dcid:{b.SOURCE}", **provenance_props}
  blocks.append(
      b.mcf_node(f"dcid:{b.PROVENANCE}",
                 typeOf="dcs:Provenance",
                 name='"Test Provenance"',
                 **provenance_props))
  return {"provenance.mcf": b.mcf(*blocks)}


# --- Registered nodes ---------------------------------------------------------
#
# Source and Provenance nodes are written once, from the node registry, which
# adds `domain` and, when missing, a `name` equal to the id.


def test_source_url_adds_a_domain():
  result = b.run_mcf(
      b.mcf_node("dcid:OtherSource",
                 typeOf="dcs:Source",
                 name='"Other Source"',
                 url='"https://data.example.com/path"'))
  b.assert_records(result.added, [
      b.record("OtherSource",
               "Source",
               name="Other Source",
               url="https://data.example.com/path",
               domain="data.example.com")
  ])


def test_provenance_without_a_name_gets_its_id_as_name():
  result = b.run_mcf(
      b.mcf_node("dcid:OtherSource2",
                 typeOf="dcs:Source",
                 url='"https://other.example.com"'),
      b.mcf_node("dcid:OtherProvenance",
                 typeOf="dcs:Provenance",
                 source="dcid:OtherSource2"))
  b.assert_records(result.added, [
      b.record("OtherSource2",
               "Source",
               name="OtherSource2",
               url="https://other.example.com",
               domain="other.example.com"),
      b.record("OtherProvenance",
               "Provenance",
               name="OtherProvenance",
               source=b.ref("OtherSource2")),
  ])


# Predicates without a dedicated field are written as they are in MCF: every
# value, with its MCF type.
_METADATA_WITH_CUSTOM_PROPERTIES = (
    b.mcf_node("dcid:OtherSource3",
               typeOf="dcs:Source",
               name='"Other Source"',
               description='"A source"',
               established="1990"),
    b.mcf_node("dcid:OtherProvenance3",
               typeOf="dcs:Provenance",
               name='"Other Provenance"',
               source="dcid:OtherSource3",
               sourceLink='"https://other.example.com/link"',
               isPartOf="dcid:CollectionA, dcid:CollectionB",
               license="undata:Other"),
)
_SOURCE_WITH_CUSTOM_PROPERTIES = b.record("OtherSource3",
                                          "Source",
                                          name="Other Source",
                                          description="A source",
                                          established=1990)
_PROVENANCE_WITH_CUSTOM_PROPERTIES = b.record(
    "OtherProvenance3",
    "Provenance",
    name="Other Provenance",
    source=b.ref("OtherSource3"),
    sourceLink="https://other.example.com/link",
    isPartOf=[b.ref("CollectionA"), b.ref("CollectionB")],
    license="undata:Other")


def test_custom_properties_of_metadata_nodes_are_kept():
  result = b.run_mcf(*_METADATA_WITH_CUSTOM_PROPERTIES)
  b.assert_records(
      result.added,
      [_SOURCE_WITH_CUSTOM_PROPERTIES, _PROVENANCE_WITH_CUSTOM_PROPERTIES])


def test_custom_properties_of_metadata_nodes_survive_the_production_path():
  result = b.run_mcf(*_METADATA_WITH_CUSTOM_PROPERTIES, mode="production")
  b.assert_records(
      result.added,
      [_SOURCE_WITH_CUSTOM_PROPERTIES, _PROVENANCE_WITH_CUSTOM_PROPERTIES])


def test_sources_are_written_to_the_folder_of_their_mcf_file():
  result = b.run_dir(_TWO_IMPORTS,
                     runner_kwargs={"import_names": ["ALL_IMPORTS"]})
  assert result.error is None, result.error
  assert _folders_of(result, "Source") == {
      "dcid:AlphaSource": ["AlphaProvenance"],
      "dcid:BetaSource": ["BetaProvenance"],
  }


def test_global_nodes_go_to_the_alphabetically_first_import_folder():
  result = b.run_dir(_TWO_IMPORTS,
                     runner_kwargs={"import_names": ["ALL_IMPORTS"]})
  assert result.error is None, result.error
  assert _folders_of(result, "StatVarGroup") == {
      "dcid:dc/g/Root": ["AlphaProvenance"]
  }


def _import_dir(name: str) -> dict[str, str]:
  """An import subdirectory whose provenance.mcf defines its own source."""
  config = {
      "inputFiles": [
          {
              "pattern": "*.mcf",
              "provenance": f"dcid:{name}Provenance"
          },
          {
              "pattern": "*.csv",
              "provenance": f"dcid:{name}Provenance",
              "columnMappings": b.OBS_MAPPINGS
          },
      ]
  }
  provenance_mcf = b.mcf(
      b.mcf_node(f"dcid:{name}Source",
                 typeOf="dcs:Source",
                 url=f'"https://{name.lower()}.example.org"'),
      b.mcf_node(f"dcid:{name}Provenance",
                 typeOf="dcs:Provenance",
                 source=f"dcid:{name}Source"))
  return {
      f"{name}/config.json": json.dumps(config),
      f"{name}/provenance.mcf": provenance_mcf,
      f"{name}/data.csv": b.csv_file(b.obs_row()),
  }


# Beta comes first so that processing order would favour it.
_TWO_IMPORTS = {**_import_dir("Beta"), **_import_dir("Alpha")}


def _folders_of(result: b.Result, type_name: str) -> dict[str, list[str]]:
  """{@id: sorted import folders} of every record of one type."""
  folders: dict[str, list[str]] = {}
  for folder, _, rec in result.output["records"]:
    if rec.get("@type") == f"dcid:{type_name}":
      folders.setdefault(rec["@id"], []).append(folder)
  return {rid: sorted(f) for rid, f in folders.items()}


# --- Provenance URL on observations -------------------------------------------

_PROVENANCE_WITH_URL = _provenance_mcf(
    url='"https://source.example.org/dataset"')


def test_provenance_url_is_copied_onto_observations():
  result = b.run_csv(_ONE_ROW, shared_files=_PROVENANCE_WITH_URL)
  b.assert_records(result.observations,
                   [b.obs(provenanceUrl="https://source.example.org/dataset")])


def test_provenance_url_survives_the_production_path():
  result = b.run_csv(_ONE_ROW,
                     mode="production",
                     shared_files=_PROVENANCE_WITH_URL)
  b.assert_records(result.observations,
                   [b.obs(provenanceUrl="https://source.example.org/dataset")])


# --- Production (multiprocessing) path ----------------------------------------
#
# In production mode each file is imported in its own worker process. MCF
# workers finish first; CSV workers then start from the provenances and
# sources the MCF files registered.

_MANY_CSVS = {
    f"case{i}.csv": b.csv_file(b.obs_row(f"country/C{i}")) for i in range(8)
}


def test_csv_workers_use_the_provenance_defined_in_mcf():
  """No CSV worker auto-creates a copy of the provenance that replaces it."""
  result = b.run(_MANY_CSVS,
                 b.obs_config(),
                 mode="production",
                 shared_files=_PROVENANCE_WITH_URL)
  b.assert_records(result.of_type("Provenance"), [])
  b.assert_records(result.of_type("Source"), [])
  b.assert_records(result.removed, [])
  b.assert_records(result.observations, [
      b.obs(f"country/C{i}", provenanceUrl="https://source.example.org/dataset")
      for i in range(8)
  ])


# --- Validation ---------------------------------------------------------------

_MISSING_SOURCES = ("Metadata Validation Failed: Linked sources are missing"
                    " for defined provenances:")

INVALID_PROVENANCES = [
    Case("inputFiles entry without a provenance",
         given={
             "inputFiles": [
                 *b.MCF_CONFIG["inputFiles"], {
                     "pattern": "case*.csv",
                     "columnMappings": b.OBS_MAPPINGS
                 }
             ]
         },
         expect=b.Fails(
             "ValueError", "Could not determine import name: missing"
             " 'provenance' configuration for file 'case.csv'.")),
    Case("provenance without a namespace prefix",
         given=b.obs_config(provenance=b.PROVENANCE),
         expect=b.Fails(
             "ValueError", "The 'provenance' property must be a valid DCID or"
             f" URI (e.g., 'dcid:FrogCensusBureau', 'custom:WHO', or a URL)."
             f" Found invalid provenance: '{b.PROVENANCE}'")),
    # The message talks about missing *sources*, and the error type is
    # GENERIC_ERROR, although the validator has a MISSING_PROVENANCE type.
    Case("provenance that no MCF defines",
         given=b.obs_config(provenance="dcid:UndefinedProvenance"),
         expect=b.Fails("ValueError", _MISSING_SOURCES)),
]


@cases(*INVALID_PROVENANCES)
def test_invalid_provenance_config(given, expect):
  result = b.run_csv(_ONE_ROW, given)
  b.assert_fails(result, expect.error_type, expect.message, expect.handshake)


def test_provenance_without_a_source_fails():
  result = b.run_csv(_ONE_ROW, shared_files=_provenance_mcf(with_source=False))
  b.assert_fails(result, "ValueError", _MISSING_SOURCES)


def test_provenance_linked_to_an_undefined_source_is_accepted():
  result = b.run_csv(_ONE_ROW,
                     shared_files=_provenance_mcf(
                         with_source=False, source="dcid:ElsewhereSource"))
  assert result.error is None, result.error
  b.assert_records(result.observations, [b.obs()])


def test_provenance_declared_only_in_the_config_sources_block_fails():
  """The legacy 'sources' config block is rejected; provenances live in MCF."""
  config = {
      "sources": {
          "Config Source": {
              "url": "https://config-source.example.org",
              "provenances": {
                  "Config Provenance": "https://config-source.example.org/p"
              },
          }
      },
      "inputFiles": [b.obs_config()["inputFiles"][-1]],
  }
  result = b.run_csv(_ONE_ROW, config, shared_files={})
  b.assert_fails(result,
                 "ValueError",
                 "Unknown key(s) in config.json: ['sources']",
                 handshake_error_types=())
