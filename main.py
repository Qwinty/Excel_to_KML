import glob
import os
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List

from openpyxl import load_workbook
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm, IntPrompt
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, SpinnerColumn
from rich.text import Text
from rich import traceback

from xlsx_to_kml import create_kml_from_coordinates, parse_coordinates, process_coordinates, transformers, create_transformer
from separator import split_excel_file_by_merges
from utils import setup_logging

# Set up rich traceback for better error display
traceback.install(show_locals=True)

# Set up logging and console
logger = setup_logging()
console = Console()

# --- Configuration ---
@dataclass
class Config:
    """Application configuration."""
    input_dir: str = "input"
    xlsx_output_dir: str = "output/xlsx"
    kml_output_dir: str = "output/kml"
    single_kml_output_dir: str = "output/kml_single"
    header_rows_count: int = 5
    merge_columns: tuple = (1, 7)  # Columns A-G

# Global config instance
config = Config()


def choose_file() -> Optional[str]:
    """Prompt user to choose an Excel file from the input directory using Rich interface."""
    input_dir = Path(config.input_dir)
    
    # Create input directory if it doesn't exist
    if not input_dir.exists():
        input_dir.mkdir(parents=True, exist_ok=True)
        console.print(Panel(
            f"[yellow]Создана папка '{input_dir}'. Пожалуйста, поместите Excel файлы в нее.[/yellow]",
            title="Информация",
            border_style="yellow"
        ))
        return None

    # Find Excel files
    files = list(input_dir.glob("*.xlsx"))

    if not files:
        console.print(Panel(
            f"[bold red]Файлы Excel (.xlsx) не найдены в папке '{input_dir}'.[/bold red]",
            title="Ошибка",
            border_style="red"
        ))
        return None

    # Create table with file information
    table = Table(title="Доступные файлы Excel", show_header=True, header_style="bold cyan")
    table.add_column("№", style="dim", width=4, justify="center")
    table.add_column("Имя файла", min_width=20)
    table.add_column("Размер", justify="right", style="green")
    table.add_column("Дата изменения", justify="center", style="blue")

    for i, file_path in enumerate(files, 1):
        size_kb = file_path.stat().st_size / 1024
        size_str = f"{size_kb:.1f} KB" if size_kb < 1024 else f"{size_kb/1024:.1f} MB"
        mod_time = file_path.stat().st_mtime
        import datetime
        mod_date = datetime.datetime.fromtimestamp(mod_time).strftime("%Y-%m-%d %H:%M")
        
        table.add_row(
            str(i),
            file_path.name,
            size_str,
            mod_date
        )

    console.print(table)

    # Get user choice with validation
    try:
        choice = IntPrompt.ask(
            "Выберите номер файла",
            choices=[str(i) for i in range(1, len(files) + 1)],
            show_choices=False
        )
        return str(files[choice - 1])
    except (KeyboardInterrupt, EOFError):
        console.print("\n[yellow]Выбор отменен.[/yellow]")
        return None


def debug_coordinate_parser():
    """Интерактивный отладочный парсер координат с выбором proj4 системы."""
    console.print(Panel(
        "[bold magenta]Режим отладки парсера координат[/bold magenta]\n"
        "[dim]Введите строки для парсинга координат и тестирования различных proj4 систем[/dim]",
        title="🔧 Отладка",
        border_style="magenta"
    ))
    
    # Включаем DEBUG уровень логирования для консоли во время отладки
    root_logger = logging.getLogger()
    console_handler = None
    original_console_level = None
    for handler in root_logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            console_handler = handler
            original_console_level = handler.level
            handler.setLevel(logging.DEBUG)
            logger.debug("Установлен DEBUG уровень логирования для консоли в режиме отладки")
            break

    try:
        while True:
            # Create mode selection menu
            mode_table = Table(title="Режимы парсинга", show_header=False, box=None)
            mode_table.add_column("№", style="bold cyan", width=3)
            mode_table.add_column("Описание", style="white")
            
            mode_table.add_row("1", "Автоматический режим (как в основной программе)")
            mode_table.add_row("2", "Ввести собственную proj4 строку")
            mode_table.add_row("3", "Вернуться в главное меню")
            
            console.print(mode_table)
            
            mode_choice = Prompt.ask(
                "Введите номер режима",
                choices=["1", "2", "3"],
                show_choices=False
            )
            
            if mode_choice == "3":
                break
                
            # Выбор proj4 системы для режима 2
            selected_transformer = None
            selected_proj4_name = None
            
            if mode_choice == "2":
                console.print(Panel(
                    "[bold cyan]Ввод собственной proj4 строки[/bold cyan]\n\n"
                    "Введите proj4 строку для преобразования координат МСК.\n\n"
                    "[dim]Пример:[/dim]\n"
                    "[yellow]+proj=tmerc +lat_0=0 +lon_0=130.71666666667 +k=1 +x_0=4300000 +y_0=-16586.442 +ellps=krass +units=m +no_defs[/yellow]",
                    title="Настройка proj4",
                    border_style="cyan"
                ))
                
                while True:
                    try:
                        custom_proj4 = Prompt.ask(
                            "\n[bold]Proj4 строка[/bold]",
                            default="",
                            show_default=False
                        ).strip()
                        
                        if not custom_proj4:
                            console.print("[yellow]Ввод не может быть пустым.[/yellow]")
                            continue
                        
                        if custom_proj4.lower() in ["back", "назад"]:
                            break
                        
                        # Проверяем, что строка начинается с +proj
                        if not custom_proj4.startswith('+proj'):
                            console.print("[yellow]Proj4 строка должна начинаться с '+proj'.[/yellow]")
                            continue
                        
                        # Пытаемся создать трансформер
                        selected_transformer = create_transformer(custom_proj4)
                        selected_proj4_name = "Пользовательская proj4"
                        
                        display_proj4 = custom_proj4[:60] + "..." if len(custom_proj4) > 60 else custom_proj4
                        console.print(f"[green]✓ Proj4 строка успешно загружена:[/green] [dim]{display_proj4}[/dim]")
                        break
                        
                    except (KeyboardInterrupt, EOFError):
                        console.print("\n[yellow]Ввод отменен.[/yellow]")
                        break
                    except Exception as e:
                        console.print(Panel(
                            f"[bold red]Ошибка при загрузке proj4 строки:[/bold red]\n{e}\n\n"
                            "[yellow]Попробуйте ввести корректную proj4 строку или введите 'back' для возврата.[/yellow]",
                            title="Ошибка",
                            border_style="red"
                        ))
                        continue
                
                if not selected_transformer:
                    continue
            
            # Основной цикл ввода строк для парсинга
            mode_text = 'Автоматический' if mode_choice == '1' else f'Ручной ({selected_proj4_name})'
            
            console.print(Panel(
                f"[bold green]Режим парсинга: {mode_text}[/bold green]\n\n"
                "Введите строку для парсинга координат.\n"
                "[dim]Для возврата к выбору режима введите 'back' или 'назад'.[/dim]",
                title="🔍 Парсинг координат",
                border_style="green"
            ))
            
            while True:
                try:
                    input_string = Prompt.ask("[bold cyan]Строка для парсинга[/bold cyan]")
                    
                    if input_string.lower() in ["back", "назад"]:
                        break
                    
                    if not input_string.strip():
                        continue
                    
                    logger.info(f"--- Начало парсинга строки: '{input_string}' ---")
                    
                    # Парсинг в зависимости от выбранного режима
                    if mode_choice == "1":
                        # Автоматический режим
                        coords, reason = parse_coordinates(input_string)
                    elif mode_choice == "2":
                        # Ручной режим с пользовательской proj4 системой
                        if (' м.' in input_string or ', м.' in input_string or input_string.endswith('м.')) and '°' not in input_string:
                            coords, reason = process_coordinates(input_string, selected_transformer)
                        else:
                            coords, reason = parse_coordinates(input_string)
                    
                    # Вывод результатов в панели
                    if reason:
                        console.print(Panel(
                            f"[bold red]Ошибка:[/bold red] {reason}",
                            title="❌ Результат парсинга",
                            border_style="red"
                        ))
                    elif not coords:
                        console.print(Panel(
                            "[yellow]Координаты не найдены или являются нулевыми.[/yellow]",
                            title="⚠️ Результат парсинга",
                            border_style="yellow"
                        ))
                    else:
                        # Создаем таблицу с результатами
                        result_table = Table(title=f"✅ Найдено {len(coords)} координат", show_header=True, header_style="bold green")
                        result_table.add_column("№", style="dim", width=3, justify="center")
                        result_table.add_column("Имя", style="cyan")
                        result_table.add_column("Долгота", style="green", justify="right")
                        result_table.add_column("Широта", style="green", justify="right")
                        
                        for i, (name, lon, lat) in enumerate(coords, 1):
                            result_table.add_row(str(i), name, f"{lon:.6f}", f"{lat:.6f}")
                        
                        console.print(result_table)
                    
                    console.print()  # Add spacing
                    
                except (KeyboardInterrupt, EOFError):
                    console.print("\n[yellow]Ввод отменен.[/yellow]")
                    break
    
    finally:
        # Восстанавливаем исходный уровень логирования консоли
        if console_handler and original_console_level is not None:
            logger.debug(f"Восстановлен исходный уровень логирования консоли: {logging.getLevelName(original_console_level)}")
            console_handler.setLevel(original_console_level)


def display_welcome():
    """Display welcome screen with application info."""
    console.print(Panel.fit(
        "[bold magenta]Конвертер Excel в KML[/bold magenta]\n\n"
        "[dim]Преобразование файлов Excel с координатами в формат KML\n"
        "для использования в картографических приложениях[/dim]",
        title="🗺️ Excel to KML Converter (RUDI.RU)",
        border_style="bright_blue",
        padding=(1, 2)
    ))

def show_main_menu() -> str:
    """Display main menu and get user choice."""
    menu_table = Table(show_header=False, box=None, padding=(0, 2))
    menu_table.add_column("№", style="bold cyan", width=3)
    menu_table.add_column("Описание", style="white")
    menu_table.add_column("Действие", style="dim")
    
    menu_table.add_row("1", "Разделить файл по регионам и преобразовать в KML", "Полный цикл обработки")
    menu_table.add_row("2", "Преобразовать один файл .xlsx в .kml", "Быстрое преобразование")
    menu_table.add_row("3", "Отладочный парсинг строк с координатами", "Тестирование парсера")
    menu_table.add_row("4", "Выход", "Завершить работу")
    
    console.print(Panel(
        menu_table,
        title="📋 Главное меню",
        border_style="cyan"
    ))
    
    return Prompt.ask(
        "Выберите режим",
        choices=["1", "2", "3", "4"],
        show_choices=False
    )

def main():
    """Main application entry point."""
    display_welcome()

    while True:
        try:
            user_input = show_main_menu()
        except (KeyboardInterrupt, EOFError):
            console.print("\n[yellow]Работа программы завершена.[/yellow]")
            break

        if user_input == "1":
            console.print(Panel(
                "[bold cyan]Режим: Разделение файла и преобразование в KML[/bold cyan]\n\n"
                "[dim]Этот режим выполнит полный цикл обработки:\n"
                "1. Разделение файла по регионам\n"
                "2. Преобразование каждого региона в KML[/dim]",
                title="🔄 Полная обработка",
                border_style="cyan"
            ))
            
            input_file = choose_file()
            if not input_file:
                continue

            input_filename = Path(input_file).name
            
            # Display processing info
            info_table = Table(show_header=False, box=None)
            info_table.add_column("Параметр", style="bold", width=30)
            info_table.add_column("Значение", style="green")
            
            info_table.add_row("Входной файл:", input_filename)
            info_table.add_row("Выход (XLSX):", config.xlsx_output_dir)
            info_table.add_row("Выход (KML):", config.kml_output_dir)
            
            console.print(Panel(
                info_table,
                title="ℹ️ Параметры обработки",
                border_style="blue"
            ))

            # --- Stage 1: Separation ---
            separation_success = False
            
            console.print("[cyan]🔄 Этап 1: Разделение файла по регионам...[/cyan]")
            
            try:
                # Ensure the separated XLSX output directory exists
                Path(config.xlsx_output_dir).mkdir(parents=True, exist_ok=True)
                logger.info(f"Создана папка для разделенных XLSX: {config.xlsx_output_dir}")

                split_excel_file_by_merges(
                    input_path=input_file,
                    output_base_dir=config.xlsx_output_dir,
                    header_rows_count=config.header_rows_count,
                    merge_cols=config.merge_columns
                )
                
                separation_success = True
                
            except Exception as e:
                console.print(Panel(
                    f"[bold red]Ошибка на этапе разделения:[/bold red]\n{e}\n\n"
                    "[dim]Проверьте, что файл не открыт в Excel и доступен для чтения.[/dim]",
                    title="❌ Ошибка этапа 1",
                    border_style="red"
                ))
                logger.exception(f"Ошибка в режиме 1 (Разделение) при обработке файла {input_file}")
            
            if separation_success:
                console.print(Panel(
                    f"[bold green]✅ Этап 1 завершен успешно[/bold green]\n\n"
                    f"Файл '[cyan]{input_filename}[/cyan]' успешно разделен.\n"
                    f"Разделенные XLSX файлы: [blue]{config.xlsx_output_dir}[/blue]",
                    title="🎉 Разделение завершено",
                    border_style="green"
                ))

            # --- Stage 2: KML Conversion (only if separation was successful) ---
            if separation_success:
                console.print(Panel(
                    "[bold cyan]Этап 2: Преобразование разделенных файлов в KML[/bold cyan]\n\n"
                    "[dim]Поиск разделенных файлов и преобразование в формат KML...[/dim]",
                    title="🔄 Этап 2",
                    border_style="cyan"
                ))
                
                # Find all .xlsx files created by the separator
                separated_files = list(Path(config.xlsx_output_dir).rglob('*.xlsx'))

                if not separated_files:
                    console.print(Panel(
                        f"[yellow]Не найдено файлов *.xlsx для преобразования в KML в директории '{config.xlsx_output_dir}' и ее подпапках.[/yellow]",
                        title="⚠️ Предупреждение",
                        border_style="yellow"
                    ))
                    continue

                console.print(f"[green]✓ Найдено {len(separated_files)} файлов .xlsx для преобразования.[/green]")

                # Ensure the KML output base directory exists
                Path(config.kml_output_dir).mkdir(parents=True, exist_ok=True)
                logger.info(f"Создана базовая папка для KML: {config.kml_output_dir}")

                conversion_errors = 0
                anomaly_files_generated = 0

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
                    # --- Use improved Progress with Rich ---
                    with Progress(
                        SpinnerColumn(),
                        TextColumn("[progress.description]{task.description}"),
                        BarColumn(),
                        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                        TextColumn("({task.completed}/{task.total} файлов)"),
                        TimeRemainingColumn(),
                        console=console,
                        transient=False
                    ) as progress:
                        # Add the conversion task
                        task = progress.add_task(
                            "Преобразование в KML...", total=len(separated_files))

                        # Loop through files
                        for xlsx_file_path in separated_files:
                            current_file = xlsx_file_path.name
                            
                            # Print filename on separate line to avoid jittering
                            console.print(f"[dim]Обработка: [cyan]{current_file}[/cyan][/dim]")
                            
                            try:
                                # Determine the relative path from the separated base dir
                                relative_path = xlsx_file_path.relative_to(Path(config.xlsx_output_dir))
                                # Construct the corresponding KML output path
                                kml_file_rel_path = relative_path.with_suffix('.kml')
                                kml_file_abs_path = Path(config.kml_output_dir) / kml_file_rel_path

                                # Ensure the target directory for the KML file exists
                                kml_file_abs_path.parent.mkdir(parents=True, exist_ok=True)

                                # Logger Debug will go to file but not console now
                                logger.debug(f"Конвертация '{xlsx_file_path}' -> '{kml_file_abs_path}'")

                                # Load workbook (ensure data_only=True)
                                workbook = load_workbook(filename=str(xlsx_file_path), data_only=True)
                                # Convert to KML and capture return value
                                created_anomaly_file = create_kml_from_coordinates(
                                    workbook.active, output_file=str(kml_file_abs_path))
                                if created_anomaly_file:
                                    anomaly_files_generated += 1

                            except Exception as e:
                                conversion_errors += 1
                                # Store error for later display (don't interrupt progress)
                                error_msg = f"Ошибка преобразования {current_file}: {e}"
                                logger.error(f"Ошибка при конвертации {xlsx_file_path} в KML: {e}", exc_info=True)
                            finally:
                                # Advance progress bar regardless of success/failure for this file
                                progress.advance(task)

                finally:
                    # --- Restore console logging level --- START
                    if console_handler and original_console_level is not None:
                        logger.debug(
                            f"Restored console log level to {logging.getLevelName(original_console_level)}")
                        console_handler.setLevel(original_console_level)
                    # --- Restore console logging level --- END

                # Reporting results (after logging level is restored)
                if conversion_errors == 0:
                    console.print(Panel(
                        f"[bold green]✅ Этап 2 завершен успешно![/bold green]\n\n"
                        f"Все {len(separated_files)} файлов успешно преобразованы в KML.\n"
                        f"KML файлы: [blue]{config.kml_output_dir}[/blue]",
                        title="🎉 Преобразование завершено",
                        border_style="green"
                    ))
                else:
                    successful_files = len(separated_files) - conversion_errors
                    
                    # Get log file path for error reference
                    log_file_path = "неизвестен"
                    if logger.handlers:
                        for handler in logger.handlers:
                            if hasattr(handler, 'baseFilename'):
                                log_file_path = str(handler.baseFilename)
                                break
                    
                    console.print(Panel(
                        f"[bold yellow]⚠️ Этап 2 завершен с ошибками[/bold yellow]\n\n"
                        f"Успешно преобразовано: [green]{successful_files}[/green] файлов\n"
                        f"Ошибок: [red]{conversion_errors}[/red]\n\n"
                        f"KML файлы: [blue]{config.kml_output_dir}[/blue]\n"
                        f"Лог-файл: [dim]{log_file_path}[/dim]",
                        title="⚠️ Преобразование завершено с ошибками",
                        border_style="yellow"
                    ))

                # Report anomaly files in a separate info panel
                if anomaly_files_generated > 0:
                    console.print(Panel(
                        f"[cyan]📊 Сгенерировано файлов с аномалиями:[/cyan] [bold]{anomaly_files_generated}[/bold]\n\n"
                        "[dim]Файлы с аномалиями (ANO_*.xlsx) содержат строки с проблемами парсинга координат.[/dim]",
                        title="📊 Статистика аномалий",
                        border_style="cyan"
                    ))
                else:
                    console.print("[dim]ℹ️ Файлы с аномалиями (ANO_*.xlsx) не генерировались.[/dim]")

        elif user_input == "2":
            console.print(Panel(
                "[bold cyan]Режим: Преобразование одного файла .xlsx в .kml[/bold cyan]\n\n"
                "[dim]Быстрое преобразование одного файла Excel в формат KML\n"
                "без разделения по регионам.[/dim]",
                title="🚀 Быстрое преобразование",
                border_style="cyan"
            ))
            
            file_name = choose_file()
            if not file_name:
                continue

            input_path = Path(file_name)
            
            # Ensure the output directory exists
            Path(config.single_kml_output_dir).mkdir(parents=True, exist_ok=True)

            # Create KML filename in the output directory
            output_filename = Path(config.single_kml_output_dir) / f"{input_path.stem}.kml"
            
            # Display processing info
            info_table = Table(show_header=False, box=None)
            info_table.add_column("Параметр", style="bold", width=20)
            info_table.add_column("Значение", style="green")
            
            info_table.add_row("Входной файл:", input_path.name)
            info_table.add_row("Выходной файл:", str(output_filename))
            
            console.print(Panel(
                info_table,
                title="ℹ️ Параметры преобразования",
                border_style="blue"
            ))

            try:
                with console.status("[cyan]Преобразование файла в KML...[/cyan]", spinner="dots"):
                    # Ensure reading only data, not formulas
                    workbook = load_workbook(filename=str(input_path), data_only=True)

                    # Convert to KML
                    created_anomaly_file = create_kml_from_coordinates(
                        workbook.active, output_file=str(output_filename))
                
                # Success message
                success_msg = f"[bold green]✅ Преобразование завершено успешно![/bold green]\n\n"
                success_msg += f"Входной файл: [cyan]{input_path.name}[/cyan]\n"
                success_msg += f"Выходной файл: [blue]{output_filename}[/blue]"
                
                if created_anomaly_file:
                    success_msg += f"\n\n[yellow]📊 Создан файл с аномалиями[/yellow]"
                
                console.print(Panel(
                    success_msg,
                    title="🎉 Готово",
                    border_style="green"
                ))
                
            except Exception as e:
                console.print(Panel(
                    f"[bold red]Ошибка при обработке файла:[/bold red]\n{e}\n\n"
                    "[dim]Проверьте, что файл не поврежден и содержит корректные данные.[/dim]",
                    title="❌ Ошибка преобразования",
                    border_style="red"
                ))
                logger.exception(f"Ошибка в режиме 2 при обработке файла {file_name}")

        elif user_input == "3":
            debug_coordinate_parser()

        elif user_input == "4":
            console.print(Panel(
                "[yellow]Спасибо за использование Excel to KML Converter![/yellow]\n\n"
                "[dim]Программа завершена.[/dim]",
                title="👋 До свидания",
                border_style="yellow"
            ))
            break


if __name__ == '__main__':
    main()
