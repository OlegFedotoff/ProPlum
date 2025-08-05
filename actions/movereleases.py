import os
import shutil
from core.utils import file_exists
from core.config import Config

def move_releases(**kwargs):
    set_name = kwargs.get('set')
    if not set_name:
        print("\n!!! Missing parameter <repository>")
        return False

    base_dir = os.path.join(os.path.abspath('.'), Config.sets_dir, set_name)
    releases_subdir = os.path.join(base_dir, "releases")

    if os.path.isdir(releases_subdir):
        print(f"\nSubdirectory 'releases' already exists for set '{set_name}'. Nothing to do.")
        return True

    print(f"\nCreating subdirectory 'releases' for set '{set_name}'...")
    try:
        os.makedirs(releases_subdir)
    except OSError as e:
        print(f"!!! Error creating subdirectory {releases_subdir}: {e}")
        return False

    print(f"Copying releases and releases.json to {releases_subdir}...")

    # List all items in the base directory
    items = [item for item in os.listdir(base_dir) if item.isdigit() and len(item) == 6]

    # Move releases.json
    releases_json_path = os.path.join(base_dir, "releases.json")
    if file_exists(releases_json_path):
        try:
            shutil.move(releases_json_path, releases_subdir)
            print(f"  Moved releases.json")
        except Exception as e:
            print(f"!!! Error moving releases.json: {e}")
            return False

    # Move release directories (e.g., 000001, 000002, etc.)
    for item in items:
        item_path = os.path.join(base_dir, item)
        # Check if it's a directory and looks like a release code (e.g., '000001')
        if os.path.isdir(item_path) and item.isdigit() and len(item) == 6:
            try:
                shutil.move(item_path, releases_subdir)
                print(f"  Moved release directory '{item}'")
            except Exception as e:
                print(f"!!! Error moving directory {item}: {e}")
                return False

    print(f"Done copying releases for set '{set_name}'.")
    return True 