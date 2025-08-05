import glob
import os
import logging

from openpyxl import load_workbook
from rich import print
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, track

from xlsx_to_kml import create_kml_from_coordinates
from separator import split_excel_file_by_merges
from utils import setup_logging

# Set up logging
logger = setup_logging()

# --- Constants for Separation ---
# These values were previously in separator.py's __main__ block
SEPARATOR_HEADER_ROW_COUNT = 5
SEPARATOR_MERGE_COLUMNS = (1, 7)  # Columns A-G
# --- End Constants ---


def choose_file() -> str | None:
    """Prompt user to choose an Excel file from the input directory."""
    input_dir = "input"
    if not os.path.exists(input_dir):
        os.makedirs(input_dir)
        print(
            f"[yellow]Создана папка '{input_dir}'. Пожалуйста, поместите Excel файлы в нее.[/yellow]")
        return None

    files = glob.glob(os.path.join(input_dir, "*.xlsx"))

    if not files:
        print(
            f"[bold red]Файлы Excel (.xlsx) не найдены в папке '{input_dir}'.[/bold red]")
        return None

    print(f"[bold cyan]Доступные файлы Excel в '{input_dir}':[/bold cyan]")
    for i, file in enumerate(files, 1):
        print(f"{i}. {os.path.basename(file)}")

    while True:
        try:
            choice_str = input("Выберите номер файла: ")
            if not choice_str:
                print("[yellow]Ввод не может быть пустым.[/yellow]")
                continue
            choice = int(choice_str)
            if 1 <= choice <= len(files):
                selected_file = files[choice - 1]
                return selected_file
            else:
                print(
                    "[yellow]Неверный номер. Пожалуйста, выберите действительный номер файла.[/yellow]")
        except ValueError:
            print("[yellow]Неверный ввод. Пожалуйста, введите число.[/yellow]")
        except EOFError:
            print("[yellow]Выбор отменен.[/yellow]")
            return None


def main():
    print("[bold magenta]Конвертер Excel в KML[/bold magenta]")

    # Define base output directories
    xlsx_separated_output_dir = "output/xlsx"
    kml_output_dir = "output/kml"

    while True:
        print("\n[bold cyan]Выберите режим:[/bold cyan]")
        print("1. Разделить файл по регионам и преобразовать в KML")
        print("2. Преобразовать один файл .xlsx в .kml (из папки 'input')")
        print("3. Выход")
        user_input = input("Введите номер режима: ")

        if user_input == "1":
            print(
                "\n[bold cyan]Режим: Разделение файла и преобразование в KML[/bold cyan]")
            input_file = choose_file()
            if not input_file:
                continue

            input_filename = os.path.basename(input_file)
            print(
                f"Выбран файл для разделения: [blue]'{input_filename}'[/blue]")
            print(
                f"Выходная директория для разделенных XLSX: [green]'{xlsx_separated_output_dir}'[/green]")
            print(
                f"Выходная директория для KML: [green]'{kml_output_dir}'[/green]")

            # --- Stage 1: Separation ---
            separation_success = False
            try:
                print("[cyan]Этап 1: Разделение файла по регионам...[/cyan]")
                # Ensure the separated XLSX output directory exists
                if not os.path.exists(xlsx_separated_output_dir):
                    os.makedirs(xlsx_separated_output_dir)
                    logger.info(
                        f"Создана папка для разделенных XLSX: {xlsx_separated_output_dir}")

                split_excel_file_by_merges(
                    input_path=input_file,
                    output_base_dir=xlsx_separated_output_dir,
                    header_rows_count=SEPARATOR_HEADER_ROW_COUNT,
                    merge_cols=SEPARATOR_MERGE_COLUMNS
                )
                print(
                    f"[bold green]Этап 1 завершен:[/bold green] Файл '{input_filename}' успешно разделен.")
                print(
                    f"Разделенные XLSX файлы находятся в папках внутри [blue]'{xlsx_separated_output_dir}'[/blue].")
                separation_success = True
            except Exception as e:
                print(
                    f"[bold red]Ошибка на этапе 1 (Разделение):[/bold red] {e}")
                logger.exception(
                    f"Ошибка в режиме 1 (Разделение) при обработке файла {input_file}")

            # --- Stage 2: KML Conversion (only if separation was successful) ---
            if separation_success:
                print(
                    "\n[cyan]Этап 2: Преобразование разделенных файлов в KML...[/cyan]")
                # Find all .xlsx files created by the separator
                separated_files = glob.glob(os.path.join(
                    xlsx_separated_output_dir, '**', '*.xlsx'), recursive=True)

                if not separated_files:
                    print(
                        f"[yellow]Не найдено файлов *.xlsx для преобразования в KML в директории '{xlsx_separated_output_dir}' и ее подпапках.[/yellow]")
                    continue

                print(
                    f"Найдено {len(separated_files)} файлов .xlsx для преобразования.")

                # Ensure the KML output base directory exists
                if not os.path.exists(kml_output_dir):
                    os.makedirs(kml_output_dir)
                    logger.info(
                        f"Создана базовая папка для KML: {kml_output_dir}")

                conversion_errors = 0
                anomaly_files_generated = 0  # Initialize counter

                # --- Temporarily suppress console logging --- START
                root_logger = logging.getLogger()
                console_handler = None
                original_console_level = None
                for handler in root_logger.handlers:
                    # Check if it's the console handler (StreamHandler)
                    # We check the type name because colorlog might be used
                    if isinstance(handler, logging.StreamHandler):
                        console_handler = handler
                        original_console_level = handler.level
                        # Set console to only show errors and above during conversion
                        handler.setLevel(logging.ERROR)
                        logger.debug(
                            f"Temporarily set console log level to {logging.getLevelName(logging.ERROR)}")
                        break
                # --- Temporarily suppress console logging --- END

                try:
                    # --- Use Progress context manager instead of track ---
                    with Progress(
                        TextColumn("[progress.description]{task.description}"),
                        BarColumn(),
                        TextColumn(
                            "[progress.percentage]{task.percentage:>3.0f}%"),
                        # Show count
                        TextColumn("({task.completed}/{task.total} файлов) ETA:"),
                        TimeRemainingColumn(),  # Estimated time remaining
                        transient=True  # Clear progress bar on exit
                    ) as progress:
                        # Add the conversion task
                        task = progress.add_task(
                            "Преобразование в KML...", total=len(separated_files))

                        # Loop through files
                        for xlsx_file_path in separated_files:
                            try:
                                # Determine the relative path from the separated base dir
                                relative_path = os.path.relpath(
                                    xlsx_file_path, xlsx_separated_output_dir)
                                # Construct the corresponding KML output path
                                kml_file_rel_path = os.path.splitext(relative_path)[
                                    0] + ".kml"
                                kml_file_abs_path = os.path.join(
                                    kml_output_dir, kml_file_rel_path)

                                # Ensure the target directory for the KML file exists
                                kml_file_dir = os.path.dirname(
                                    kml_file_abs_path)
                                if not os.path.exists(kml_file_dir):
                                    os.makedirs(kml_file_dir)

                                # Logger Debug will go to file but not console now
                                logger.debug(
                                    f"Конвертация '{xlsx_file_path}' -> '{kml_file_abs_path}'")

                                # Load workbook (ensure data_only=True)
                                workbook = load_workbook(
                                    filename=xlsx_file_path, data_only=True)
                                # Convert to KML and capture return value
                                created_anomaly_file = create_kml_from_coordinates(
                                    workbook.active, output_file=kml_file_abs_path)
                                if created_anomaly_file:
                                    anomaly_files_generated += 1  # Increment counter

                            except Exception as e:
                                conversion_errors += 1
                                # Print the error to console directly since logging might be suppressed
                                print(
                                    f"[bold red]Ошибка преобразования {os.path.basename(xlsx_file_path)} в KML:[/bold red] {e}")
                                # Log the error (will go to file and console if level is ERROR or higher)
                                logger.error(
                                    f"Ошибка при конвертации {xlsx_file_path} в KML: {e}", exc_info=True)
                            finally:
                                # Advance progress bar regardless of success/failure for this file
                                progress.update(task, advance=1)
                    # --- End Progress context manager ---

                finally:
                    # --- Restore console logging level --- START
                    if console_handler and original_console_level is not None:
                        logger.debug(
                            f"Restored console log level to {logging.getLevelName(original_console_level)}")
                        console_handler.setLevel(original_console_level)
                    # --- Restore console logging level --- END

                # Reporting results (after logging level is restored)
                if conversion_errors == 0:
                    print(
                        f"[bold green]Этап 2 завершен:[/bold green] Все {len(separated_files)} файлов успешно преобразованы в KML.")
                    print(
                        f"KML файлы находятся в папках внутри [blue]'{kml_output_dir}'[/blue].")
                else:
                    print(
                        f"[bold yellow]Этап 2 завершен с {conversion_errors} ошибками.[/bold yellow]")
                    print(
                        f"Успешно преобразовано {len(separated_files) - conversion_errors} файлов.")
                    print(
                        f"KML файлы находятся в папках внутри [blue]'{kml_output_dir}'[/blue]. Проверьте лог-файл ({logger.handlers[0].baseFilename if logger.handlers else 'log'}) для деталей ошибок.")

                # Report the number of anomaly files generated
                if anomaly_files_generated > 0:
                    print(
                        f"[cyan]Сгенерировано файлов с аномалиями (ANO_*.xlsx):[/cyan] {anomaly_files_generated}")
                else:
                    print(
                        f"[cyan]Файлы с аномалиями (ANO_*.xlsx) не генерировались.[/cyan]")

        elif user_input == "2":
            print(
                "\n[bold cyan]Режим: Преобразование одного файла .xlsx в .kml[/bold cyan]")
            file_name = choose_file()
            if not file_name:
                continue

            print(f"Выбран файл: [blue]'{os.path.basename(file_name)}'[/blue]")

            # Define the output directory for single KML files (can be the same base)
            # Or use kml_output_dir if preferred
            single_kml_output_dir = "output/kml_single"

            # Ensure the output directory exists
            if not os.path.exists(single_kml_output_dir):
                os.makedirs(single_kml_output_dir)
                print(
                    f"Создана директория для вывода: [green]'{single_kml_output_dir}'[/green]")

            # Create KML filename in the output directory
            base_name = os.path.basename(file_name)
            base_name_without_ext = os.path.splitext(base_name)[0]
            output_filename = os.path.join(
                single_kml_output_dir, f"{base_name_without_ext}.kml")
            print(f"Выходной файл: [green]'{output_filename}'[/green]")

            try:
                # Ensure reading only data, not formulas
                workbook = load_workbook(filename=file_name, data_only=True)

                # Pass workbook.active (the sheet) and output filename
                # Removed sort_numbers as it wasn't used in the example
                create_kml_from_coordinates(
                    workbook.active, output_file=output_filename)  # Removed sort_numbers=[] argument
                print(
                    f"[bold green]Успешно преобразовано[/bold green] '{os.path.basename(file_name)}' [bold green]в[/bold green] '{output_filename}'")
            except Exception as e:
                print(
                    f"[bold red]Ошибка обработки {file_name}:[/bold red] {e}")
                logger.exception(
                    f"Ошибка в режиме 2 при обработке файла {file_name}")

        elif user_input == "3":
            print("\n[yellow]Выход из программы.[/yellow]")
            break

        else:
            print("[yellow]Неверный ввод. Пожалуйста, введите 1, 2 или 3.[/yellow]")


if __name__ == '__main__':
    main()
