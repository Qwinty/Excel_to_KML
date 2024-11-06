import glob
import os

from openpyxl import load_workbook

from xlsx_to_kml import create_kml_from_coordinates


def choose_file() -> str | None:
    """Prompt user to choose an Excel file from the current directory."""
    files = glob.glob("input/*.xlsx")

    if not files:
        print("No Excel files found in the current directory.")
        return None

    for i, file in enumerate(files, 1):
        print(f"{i}. {file}")

    while True:
        try:
            choice = int(input("Choose a file number: "))
            if 1 <= choice <= len(files):
                return files[choice - 1]
            else:
                print("Invalid number. Please choose a valid file number.")
        except ValueError:
            print("Invalid input. Please enter a number.")


def batch_convert(dir):
    # Converts all files in directory.
    files = glob.glob(f"{dir}/*.xlsx")
    print(f"Found {len(files)} files... In dir: {dir}")

    # Making subdirectory
    kml_dir = os.path.join(dir, "kml")
    if not os.path.exists(kml_dir):
        os.makedirs(kml_dir)

    for file in files:
        print(f"Working on {file}...")
        # Get the base filename without extension
        base_name = os.path.basename(file)
        base_name_without_ext = os.path.splitext(base_name)[0]

        # Create output path in the kml subdirectory
        out_filename = os.path.join(kml_dir, f"{base_name_without_ext}.kml")
        print(f"Output file: {out_filename}")

        workbook = load_workbook(filename=file, data_only=True)
        create_kml_from_coordinates(workbook.active, output_file=out_filename)
        exit()

def main():
    # file_name = choose_file()
    # if not file_name:
    #     return
    # workbook = load_workbook(filename=file_name, data_only=True)
    #
    # # Specify which "№ п/п" values should have their coordinates sorted
    # sort_numbers = []  # Add the specific numbers you want to sort
    # filename = file_name.rsplit(".", 1)[0] + ".kml"
    # create_kml_from_coordinates(workbook.active, output_file=filename, sort_numbers=sort_numbers)

    batch_convert("output/separated_regions")


if __name__ == '__main__':
    # test = parse_coordinates("""МСК-63 зона 1 г.о. Самара, Куйбышевского района, Самарской области, на левом берегу реки на 1 км от устья 1: 381631.8м., 1368949.26м.""")
    # print(test)
    main()
