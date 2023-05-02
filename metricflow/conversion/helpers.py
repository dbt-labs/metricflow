import os
import json

def found_dbt_project() -> str:
    """Checks if dbt_project.yml exists in the current directory and returns a boolean."""
    try:
        os.path.exists("dbt_project.yml")
        return "Completion: Found the dbt project file!"
    except:
        return "Error: dbt_project.yml not found in current directory. Please navigate to a dbt directory to run conversion"

def check_manifest_file():
    """This function checks if the manifest.json file in the 'target/' directory is
    newer or the same age as all other files in the current directory. If any other
    file is more recent, it returns a message to update the manifest file. The 
    function uses os.scandir() for improved performance."""
    target_directory = 'target/'
    manifest_path = os.path.join(target_directory, 'manifest.json')
    if not os.path.isfile(manifest_path):
        return "The manifest.json file does not exist."

    manifest_mtime = os.path.getmtime(manifest_path)
    ignored_files = get_ignored_files()

    for root, _, files in os.walk("."):
        for file in files:
            if file not in ignored_files:
                file_path = os.path.join(root, file)
                file_mtime = os.path.getmtime(file_path)

                if file_mtime > manifest_mtime + 60:
                    return "Your manifest.json file is out of date - please run dbt compile again!"

    return "Your manifest.json file is up to date."

def get_ignored_files():
    ignored_files = set()
    if os.path.isfile(".gitignore"):
        with open(".gitignore", "r") as ignore_file:
            for line in ignore_file.readlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    ignored_files.add(line)
    return ignored_files

def extract_keys_from_manifest():
    """The function reads the manifest.json file from the 'target/' directory and
    returns only the top-level keys "metrics" and "semantic_models". The function
    uses the built-in json library for parsing the JSON data."""
    target_directory = 'target/'
    manifest_path = os.path.join(target_directory, 'manifest.json')
    if not os.path.isfile(manifest_path):
        return "The manifest.json file does not exist."

    with open(manifest_path, 'r') as manifest_file:
        manifest_data = json.load(manifest_file)

    extracted_data = {key: manifest_data[key] for key in ['metrics', 'semantic_models'] if key in manifest_data}
    return extracted_data