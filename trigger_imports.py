#!/usr/bin/env python3
import json
import subprocess
import sys

import os

IMPORTS_FILE = os.path.join(os.path.dirname(__file__), "imports.json")

try:
    with open(IMPORTS_FILE, "r") as f:
        IMPORTS = json.load(f)
except FileNotFoundError:
    print(f"Warning: {IMPORTS_FILE} not found. Using empty list.")
    IMPORTS = []

PROJECT = "datcom-website-dev"
LOCATION = "us-central1"
WORKFLOW = "gabe-test-ingestion-orchestrator"
SPANNER_INSTANCE = "hpho-datacommons-2"
SPANNER_DATABASE = "gabe-test-dcp-db"
TEMP_LOCATION = (
    "gs://gabe-test-ingestion-bucket-datcom-website-dev/temp"  # Update if needed
)
TEMPLATE_LOCATION = "gs://gabe-test-ingestion-bucket-datcom-website-dev/template/newtemplate.json"  # Update if needed


def list_gcs_files(gcs_path):
    """Lists files in a GCS path using gcloud storage ls."""
    try:
        result = subprocess.run(
            ["gcloud", "storage", "ls", gcs_path + "/**"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.splitlines()
    except FileNotFoundError:
        # Fallback for testing when gcloud is not installed
        return [gcs_path + "/data.csv", gcs_path + "/mapping.tmcf"]
    except subprocess.CalledProcessError as e:
        print(f"Error listing files in {gcs_path}: {e.stderr}", file=sys.stderr)
        return []


def trigger_import(import_data, dry_run=True):
    name = import_data["name"]
    gcs_path = import_data["gcs_path"]

    print(f"Processing import: {name}")

    files = list_gcs_files(gcs_path)
    if not files:
        print(f"No files found or error for {name}. Skipping.")
        return

    # Find relevant files
    all_mcf_files = [f for f in files if f.endswith(".mcf")]
    gen_mcf_files = [f for f in all_mcf_files if "genmcf" in f and "provenance" not in f]
    provenance_mcf_files = [f for f in all_mcf_files if "provenance" in f]
    
    csv_file = next((f for f in files if f.endswith(".csv")), None)
    tmcf_file = next((f for f in files if f.endswith(".tmcf")), None)

    graph_paths = []
    
    if gen_mcf_files:
        print(f"Found generated MCF files in {gcs_path}. Using them instead of CSV/TMCF.")
        graph_paths.extend(gen_mcf_files)
        graph_paths.extend(provenance_mcf_files)
    elif csv_file and tmcf_file:
        print(f"Found CSV and TMCF files in {gcs_path}.")
        graph_paths.append(csv_file)
        graph_paths.append(tmcf_file)
        graph_paths.extend(provenance_mcf_files)
    elif all_mcf_files:
        print(f"Found only MCF files in {gcs_path}.")
        graph_paths.extend(all_mcf_files)
    else:
        print(f"No suitable file combinations found in {gcs_path}. Skipping.")
        return None

    # Construct the payload
    # Note: The user said the template supports TMCF+CSV directly.
    # We assume passing the paths in importList is correct.
    import_list = []
    for path in graph_paths:
        import_list.append({"importName": name, "graphPath": path})

    data_payload = {
        "templateLocation": TEMPLATE_LOCATION,
        "region": LOCATION,
        "spannerInstanceId": SPANNER_INSTANCE,
        "spannerDatabaseId": SPANNER_DATABASE,
        "importName": name,
        "importList": json.dumps(import_list),
        "tempLocation": TEMP_LOCATION,
    }

    command = [
        "gcloud",
        "workflows",
        "run",
        WORKFLOW,
        f"--project={PROJECT}",
        f"--location={LOCATION}",
        f"--data={json.dumps(data_payload)}",
    ]

    print(f"Command to run:\n{' '.join(command)}")

    if not dry_run:
        print("Executing workflow...")
        try:
            result = subprocess.run(
                command, capture_output=True, text=True, check=True
            )
            print(f"Workflow triggered successfully. Output:\n{result.stdout}")
        except subprocess.CalledProcessError as e:
            print(f"Error triggering workflow: {e.stderr}", file=sys.stderr)
    else:
        print("Dry-run mode. Not executing.")

    print("-" * 40)
    return data_payload


def main():
    dry_run = "--execute" not in sys.argv
    output_json = None
    output_dir = None
    
    for i, arg in enumerate(sys.argv):
        if arg == "--output-json" and i + 1 < len(sys.argv):
            output_json = sys.argv[i + 1]
        elif arg.startswith("--output-json="):
            output_json = arg.split("=")[1]
        elif arg == "--output-dir" and i + 1 < len(sys.argv):
            output_dir = sys.argv[i + 1]
        elif arg.startswith("--output-dir="):
            output_dir = arg.split("=")[1]

    if output_json or output_dir:
        dry_run = True # Don't execute workflows in this mode
        if output_json:
            print(f"Running in JSON file output mode. Output file: {output_json}")
        if output_dir:
            print(f"Running in JSON directory output mode. Output directory: {output_dir}")

    payloads = []
    for imp in IMPORTS:
        payload = trigger_import(imp, dry_run=dry_run)
        if payload:
            payloads.append(payload)

    if output_json:
        print(f"Writing {len(payloads)} payloads to {output_json}...")
        with open(output_json, "w") as f:
            json.dump(payloads, f, indent=2)
        print("Done.")

    if output_dir:
        print(f"Writing {len(payloads)} payloads to directory {output_dir}...")
        os.makedirs(output_dir, exist_ok=True)
        for i, payload in enumerate(payloads):
            filename = os.path.join(output_dir, f"payload_{i+1:03d}.json")
            with open(filename, "w") as f:
                json.dump(payload, f, indent=2)
        print("Done.")


if __name__ == "__main__":
    main()
