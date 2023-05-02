import os
import subprocess

 
## Instead of running dbt commands, lets recurse through the file path and see if 
## the manifest was updated more recently than files have been changed. If so, 
## warn people that they need to compile their project.

def found_dbt_project() -> str:
    """Checks if dbt_project.yml exists in the current directory and returns a boolean."""
    try:
        os.path.exists("dbt_project.yml")
        return "Completion: Found the dbt project file!"
    except:
        return "Error: dbt_project.yml not found in current directory. Please navigate to a dbt directory to run conversion"

def check_manifest_file():
    target_directory = 'target/'
    manifest_path = os.path.join(target_directory, 'manifest.json')
    if not os.path.isfile(manifest_path):
        return "The manifest.json file does not exist."

    manifest_mtime = os.path.getmtime(manifest_path)

    with os.scandir() as entries:
        for entry in entries:
            if entry.is_file():
                file_mtime = os.path.getmtime(entry)

                if file_mtime > manifest_mtime + 60:
                    return "Your manifest.json file is out of date - please run dbt compile again!"

    return "Your manifest.json file is up to date."