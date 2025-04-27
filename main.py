import glob
import os
import logging

from openpyxl import load_workbook

from xlsx_to_kml import create_kml_from_coordinates
from utils import setup_logging

# Set up logging
logger = setup_logging()


def choose_file() -> str | None:
    """Prompt user to choose an Excel file from the current directory."""
    files = glob.glob("input/*.xlsx")

    if not files:
        logger.warning("No Excel files found in the current directory.")
        print("No Excel files found in the current directory.")
        return None

    for i, file in enumerate(files, 1):
        print(f"{i}. {file}")

    while True:
        try:
            choice = int(input("Choose a file number: "))
            if 1 <= choice <= len(files):
                logger.info(f"Selected file: {files[choice - 1]}")
                return files[choice - 1]
            else:
                logger.warning(f"Invalid number chosen: {choice}")
                print("Invalid number. Please choose a valid file number.")
        except ValueError:
            logger.warning("User entered non-numeric value")
            print("Invalid input. Please enter a number.")


def batch_convert(dir):
    # Converts all files in directory.
    files = glob.glob(f"{dir}/*.xlsx")
    logger.info(f"Found {len(files)} files in directory: {dir}")
    print(f"Found {len(files)} files... In dir: {dir}")

    # Making subdirectory
    kml_dir = os.path.join(dir, "kml")
    if not os.path.exists(kml_dir):
        logger.info(f"Creating directory: {kml_dir}")
        os.makedirs(kml_dir)

    for file in files:
        logger.info(f"Processing file: {file}")
        print(f"Working on {file}...")
        # Get the base filename without extension
        base_name = os.path.basename(file)
        base_name_without_ext = os.path.splitext(base_name)[0]

        # Create output path in the kml subdirectory
        out_filename = os.path.join(kml_dir, f"{base_name_without_ext}.kml")
        logger.info(f"Output file will be: {out_filename}")
        print(f"Output file: {out_filename}")

        try:
            workbook = load_workbook(filename=file, data_only=True)
            create_kml_from_coordinates(
                workbook.active, output_file=out_filename)
            logger.info(f"Successfully converted {file} to {out_filename}")
        except Exception as e:
            logger.error(f"Error converting {file}: {str(e)}", exc_info=True)
        exit()


def main():
    logger.info("Starting application")

    user_input = input(
        "1. Single convert \n2. Batch convert separated_regions\n")
    logger.info(f"User selected option: {user_input}")

    if user_input == "1":
        logger.info("Selected single file conversion")
        file_name = choose_file()
        if not file_name:
            logger.warning("No file selected, exiting")
            return

        try:
            workbook = load_workbook(filename=file_name, data_only=True)

            # Specify which "№ п/п" values should have their coordinates sorted
            sort_numbers = []  # Add the specific numbers you want to sort
            filename = file_name.rsplit(".", 1)[0] + ".kml"
            logger.info(f"Converting {file_name} to {filename}")

            create_kml_from_coordinates(
                workbook.active, output_file=filename, sort_numbers=sort_numbers)
            logger.info(f"Successfully converted {file_name} to {filename}")
        except Exception as e:
            logger.error(
                f"Error processing {file_name}: {str(e)}", exc_info=True)

    elif user_input == "2":
        logger.info("Selected batch conversion of separated regions")
        batch_convert("output/separated_regions")

    else:
        logger.warning(f"Invalid input: {user_input}")
        print("Invalid input. Please enter 1 or 2.")

    logger.info("Application finished")


if __name__ == '__main__':
    main()
