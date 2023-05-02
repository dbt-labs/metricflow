import os
import time
import json
from metricflow.conversion.helpers import check_manifest_file, extract_semantic_manifest

# Unit Test
def test_check_manifest_file_missing_manifest(tmpdir):
    os.chdir(tmpdir)
    result = check_manifest_file()
    assert result == "The manifest.json file does not exist."

# Integration Test
def test_check_manifest_file(tmpdir):
    os.chdir(tmpdir)
    target_directory = 'target/'
    os.makedirs(target_directory)

    manifest_path = os.path.join(target_directory, 'manifest.json')
    with open(manifest_path, 'w') as manifest_file:
        manifest_file.write("{}")

    # Touch a file with an older timestamp
    with open('older_file.txt', 'w') as older_file:
        older_file.write("Test data")

    time.sleep(6)

    # Update manifest.json file with new content
    new_manifest_content = '{"new_key": "new_value"}'
    with open(manifest_path, 'w') as manifest_file:
        manifest_file.write(new_manifest_content)

    result = check_manifest_file()
    assert result == "Your manifest.json file is up to date."

    # Touch a file with a newer timestamp
    time.sleep(6)
    with open('newer_file.txt', 'w') as newer_file:
        newer_file.write("Test data")

    result = check_manifest_file()
    assert result == "Your manifest.json file is out of date - please run dbt compile again!"


def test_extract_semantic_manifest(tmpdir):
    os.chdir(tmpdir)
    target_directory = 'target/'
    os.makedirs(target_directory)

    manifest_content = {
        "metrics": {"metric_1": "value_1", "metric_2": "value_2"},
        "semantic_models": {"model_1": "value_1", "model_2": "value_2"},
        "models": {"key_1": "value_1", "key_2": "value_2"},
    }

    manifest_path = os.path.join(target_directory, 'manifest.json')
    with open(manifest_path, 'w') as manifest_file:
        json.dump(manifest_content, manifest_file)


    semantic_manifest = extract_semantic_manifest()

    assert semantic_manifest == {
        "metrics": {"metric_1": "value_1", "metric_2": "value_2"},
        "semantic_models": {"model_1": "value_1", "model_2": "value_2"},
    }