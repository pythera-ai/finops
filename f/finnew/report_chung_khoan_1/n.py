import subprocess
import os


def get_linux_fonts_fc_list():
    font_names = set()
    font_paths = set()
    try:
        # Get font names (family names)
        process_names = subprocess.Popen(
            ["fc-list", ":", "family"], stdout=subprocess.PIPE, text=True
        )
        stdout_names, _ = process_names.communicate()
        for line in stdout_names.splitlines():
            # fc-list output can have multiple families per line, comma-separated
            families = line.split(",")[0].strip()  # Take the primary family name
            if families:
                font_names.add(families)

        # Get font file paths
        process_paths = subprocess.Popen(
            ["fc-list", ":", "file"], stdout=subprocess.PIPE, text=True
        )
        stdout_paths, _ = process_paths.communicate()
        for line in stdout_paths.splitlines():
            path = line.strip().rstrip(":")  # Remove trailing colon if present
            if path and os.path.exists(
                path
            ):  # Ensure path is not empty and file exists
                font_paths.add(path)

    except FileNotFoundError:
        print("fc-list command not found. Ensure fontconfig is installed.")
    except Exception as e:
        print(f"Error running fc-list: {e}")
    return sorted(list(font_names)), sorted(list(font_paths))


def main():
    if os.name == "posix":  # Typically Linux or macOS
        # On macOS, fc-list might not be default, matplotlib is better
        # This is more geared towards Linux with fontconfig
        names, paths = get_linux_fonts_fc_list()
        if names:
            print("--- Linux Font Names (fc-list) ---")
            for name in names:
                print(name)
        if paths:
            print("\n--- Linux Font Paths (fc-list) ---")
            for path in paths:
                print(path)
    else:
        print("fc-list method is for Linux.")
