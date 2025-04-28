import glob
import os
import logging

from openpyxl import load_workbook
from rich import print
from rich.progress import track

from xlsx_to_kml import create_kml_from_coordinates
from utils import setup_logging

# Set up logging
logger = setup_logging()


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


def batch_convert(input_dir: str, output_dir_base: str):
    """Converts all Excel files in the input directory."""
    if not os.path.exists(input_dir):
        print(
            f"[bold red]Ошибка: Директория для пакетной обработки не найдена:[/bold red] {input_dir}")
        return

    files = glob.glob(os.path.join(input_dir, "*.xlsx"))
    if not files:
        print(
            f"[bold red]Файлы Excel не найдены в директории:[/bold red] {input_dir}")
        return

    print(
        f"Найдено {len(files)} файлов в [blue]'{input_dir}'[/blue]. Запуск пакетного преобразования...")

    # Ensure the base output directory exists
    if not os.path.exists(output_dir_base):
        os.makedirs(output_dir_base)
        print(
            f"Создана базовая директория для вывода: [green]'{output_dir_base}'[/green]")

    # Create a subdirectory within the output base for these KML files
    # Use the input directory's name for the subdirectory
    input_dir_name = os.path.basename(os.path.normpath(input_dir))
    kml_output_dir = os.path.join(output_dir_base, input_dir_name + "_kml")
    if not os.path.exists(kml_output_dir):
        os.makedirs(kml_output_dir)
        print(
            f"Создана директория для вывода KML: [green]'{kml_output_dir}'[/green]")
    else:
        print(f"Директория для вывода: [green]'{kml_output_dir}'[/green]")

    # Use rich.progress.track for a progress bar
    for file in track(files, description=f"Преобразование файлов из {input_dir}..."):
        # Get the base filename without extension
        base_name = os.path.basename(file)
        base_name_without_ext = os.path.splitext(base_name)[0]

        # Create output path in the kml subdirectory
        out_filename = os.path.join(
            kml_output_dir, f"{base_name_without_ext}.kml")

        try:
            # Ensure reading only data, not formulas
            workbook = load_workbook(filename=file, data_only=True)
            # Pass workbook.active (the sheet) and output filename
            create_kml_from_coordinates(
                workbook.active, output_file=out_filename)
        except Exception as e:
            print(f"[bold red]Ошибка преобразования {file}:[/bold red] {e}")

    print(
        f"[bold green]Пакетное преобразование для '{input_dir}' завершено.[/bold green] KML файлы находятся в [blue]'{kml_output_dir}'[/blue].")


def main():
    print("[bold magenta]Конвертер Excel в KML[/bold magenta]")

    # Define base output directory
    output_base_dir = "output"

    while True:
        print("\n[bold cyan]Выберите режим:[/bold cyan]")
        print("1. Обработать и разделить файл по регионам (.xlsx + .kml)")
        print("2. Преобразовать один файл .xlsx в .kml (из папки 'input')")
        print("3. Выход")
        user_input = input("Введите номер режима: ")

        if user_input == "1":
            print(
                "[bold cyan]Режим: Обработка и разделение файла по регионам[/bold cyan]")
            file_name = choose_file()
            if not file_name:
                continue

            print(
                f"Выбран файл для разделения: [blue]'{os.path.basename(file_name)}'[/blue]")
            print(
                f"Выходная директория для регионов: [green]'{os.path.join(output_base_dir, 'separated_regions')}'[/green]")

            try:
                # processor = WaterObjectsProcessor(file_name, output_base_dir)
                # processor.process_file()
                print(
                    f"[bold green]Файл '{os.path.basename(file_name)}' успешно обработан и разделен.[/bold green]")
                print(
                    f"Результаты находятся в подпапках директории [blue]'{output_base_dir}'[/blue].")
            except Exception as e:
                print(
                    f"[bold red]Ошибка при обработке и разделении файла {file_name}:[/bold red] {e}")
                logger.exception(
                    f"Ошибка в режиме 1 при обработке файла {file_name}")

        elif user_input == "2":
            print(
                "[bold cyan]Режим: Преобразование одного файла .xlsx в .kml[/bold cyan]")
            file_name = choose_file()
            if not file_name:
                continue

            print(f"Выбран файл: [blue]'{os.path.basename(file_name)}'[/blue]")

            # Ensure the base output directory exists
            if not os.path.exists(output_base_dir):
                os.makedirs(output_base_dir)
                print(
                    f"Создана базовая директория для вывода: [green]'{output_base_dir}'[/green]")

            # Create KML filename in the output directory
            base_name = os.path.basename(file_name)
            base_name_without_ext = os.path.splitext(base_name)[0]
            output_filename = os.path.join(
                output_base_dir, f"{base_name_without_ext}.kml")
            print(f"Выходной файл: [green]'{output_filename}'[/green]")

            try:
                # Ensure reading only data, not formulas
                workbook = load_workbook(filename=file_name, data_only=True)

                # Specify which "№ п/п" values should have their coordinates sorted (kept empty as before)
                sort_numbers = []

                # Pass workbook.active (the sheet) and output filename
                create_kml_from_coordinates(
                    workbook.active, output_file=output_filename, sort_numbers=sort_numbers)
                print(
                    f"[bold green]Успешно преобразовано[/bold green] '{os.path.basename(file_name)}' [bold green]в[/bold green] '{output_filename}'")
            except Exception as e:
                print(
                    f"[bold red]Ошибка обработки {file_name}:[/bold red] {e}")

        elif user_input == "3":
            print("[yellow]Выход из программы.[/yellow]")
            break

        else:
            print("[yellow]Неверный ввод. Пожалуйста, введите 1, 2 или 3.[/yellow]")


if __name__ == '__main__':
    main()
