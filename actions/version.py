"""
HDSET Release Management Framework Version Action
"""

HDSET_VERSION = "2.12"
HDSET_NAME = "HDSET Release Management Framework"

def get_version():
    """Returns the current version of HDSET framework"""
    return HDSET_VERSION

def get_full_version_info():
    """Returns full version information"""
    return f"{HDSET_NAME} v{HDSET_VERSION}"

def print_version():
    """Prints version information to console"""
    print(f"{HDSET_NAME}")
    print(f"Version: {HDSET_VERSION}")
    print("=" * 40)

def version(**kwargs):
    """Version action function - shows framework version"""
    print_version() 