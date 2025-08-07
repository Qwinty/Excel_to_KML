import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, SpinnerColumn

from src.config import Config
from src.stats import ProcessingStats, display_processing_statistics
from src.ui import console, choose_file
from src.separator import split_excel_file_by_merges
from src.workers import initialize_worker_logging, process_file_worker


logger = logging.getLogger(__name__)


def process_mode_1_full_processing(config: Config) -> None:
    console.print(Panel(
        "[bold cyan]Режим: Разделение файла и преобразование в KML[/bold cyan]\n\n"
        "[dim]Этот режим выполнит полный цикл обработки:\n"
        "1. Разделение файла по регионам\n"
        "2. Преобразование каждого региона в KML[/dim]",
        title="🔄 Полная обработка",
        border_style="cyan"
    ))

    input_file = choose_file(config)
    if not input_file:
        return

    input_filename = Path(input_file).name

    processing_stats = ProcessingStats()

    # Stage 1: Separation
    separation_success = _process_file_separation(input_file, input_filename, processing_stats, config)

    # Stage 2: KML Conversion
    if separation_success:
        _process_kml_conversion(processing_stats, config)
        display_processing_statistics(processing_stats)


def _process_file_separation(input_file: str, input_filename: str, processing_stats: ProcessingStats, config: Config) -> bool:
    separation_success = False

    console.print("[cyan]🔄 Этап 1: Разделение файла по регионам...[/cyan]")

    try:
        Path(config.xlsx_output_dir).mkdir(parents=True, exist_ok=True)
        logger.info(f"Создана папка для разделенных XLSX: {config.xlsx_output_dir}")

        split_excel_file_by_merges(
            input_path=input_file,
            output_base_dir=config.xlsx_output_dir,
            header_rows_count=config.header_rows_count,
            merge_cols=config.merge_columns
        )

        separated_files = list(Path(config.xlsx_output_dir).rglob('*.xlsx'))
        processing_stats.regions_detected = len(separated_files)
        processing_stats.files_created = [str(f) for f in separated_files]

        separation_success = True

    except Exception as e:
        console.print(Panel(
            f"[bold red]Ошибка на этапе разделения:[/bold red]\n{e}\n\n"
            "[dim]Проверьте, что файл не открыт в Excel и доступен для чтения.[/dim]",
            title="❌ Ошибка этапа 1",
            border_style="red"
        ))
        logger.exception(
            f"Ошибка в режиме 1 (Разделение) при обработке файла {input_file}")

    if separation_success:
        console.print(Panel(
            f"[bold green]✅ Этап 1 завершен успешно[/bold green]\n\n"
            f"Файл '[cyan]{input_filename}[/cyan]' успешно разделен.\n"
            f"Разделенные XLSX файлы: [blue]{config.xlsx_output_dir}[/blue]",
            title="🎉 Разделение завершено",
            border_style="green"
        ))

    return separation_success


def _process_kml_conversion(processing_stats: ProcessingStats, config: Config) -> None:
    console.print(Panel(
        "[bold cyan]Этап 2: Преобразование разделенных файлов в KML[/bold cyan]\n\n"
        "[dim]Поиск разделенных файлов и преобразование в формат KML...[/dim]",
        title="🔄 Этап 2",
        border_style="cyan"
    ))

    separated_files = list(Path(config.xlsx_output_dir).rglob('*.xlsx'))

    if not separated_files:
        console.print(Panel(
            f"[yellow]Не найдено файлов *.xlsx для преобразования в KML в директории '{config.xlsx_output_dir}' и ее подпапках.[/yellow]",
            title="⚠️ Предупреждение",
            border_style="yellow"
        ))
        return

    console.print(f"[green]✓ Найдено {len(separated_files)} файлов .xlsx для преобразования.[/green]")

    Path(config.kml_output_dir).mkdir(parents=True, exist_ok=True)
    logger.info(f"Создана базовая папка для KML: {config.kml_output_dir}")

    conversion_errors = _run_parallel_conversion(separated_files, processing_stats, config)
    _report_conversion_results(separated_files, conversion_errors, config)


def _run_parallel_conversion(separated_files: List[Path], processing_stats: ProcessingStats, config: Config) -> int:
    conversion_errors = 0

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
        task = progress.add_task("Преобразование в KML...", total=len(separated_files))

        worker_args = _prepare_worker_args(separated_files, config)
        max_workers = _determine_max_workers(separated_files, config)

        console.print(f"[dim]Запуск параллельной обработки с {max_workers} потоками...[/dim]")
        console.print(f"[dim]DEBUG/WARNING сообщения подавлены в консоли для повышения производительности[/dim]")

        from src.workers import process_file_worker  # ensure correct import in subprocess on Windows

        with ProcessPoolExecutor(
            max_workers=max_workers,
            initializer=initialize_worker_logging
        ) as executor:
            future_to_file = {
                executor.submit(process_file_worker, **args): args['xlsx_file_path']
                for args in worker_args
            }

            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                filename = Path(file_path).name

                try:
                    success, processed_filename, conversion_result, error_message = future.result()

                    if success:
                        console.print(f"[dim]Завершено: [green]{processed_filename}[/green][/dim]")
                        if conversion_result is not None:
                            processing_stats.add_file_result(conversion_result)
                            if conversion_result.anomaly_file_created:
                                processing_stats.anomaly_files_generated += 1
                    else:
                        console.print(f"[dim]Ошибка: [red]{processed_filename}[/red][/dim]")
                        conversion_errors += 1
                        processing_stats.conversion_errors += 1
                        logger.error(f"Ошибка при конвертации {file_path} в KML: {error_message}")

                except Exception as e:
                    console.print(f"[dim]Критическая ошибка: [red]{filename}[/red][/dim]")
                    conversion_errors += 1
                    processing_stats.conversion_errors += 1
                    logger.error(f"Критическая ошибка при обработке {file_path}: {e}", exc_info=True)
                finally:
                    progress.advance(task)

    return conversion_errors


def _prepare_worker_args(separated_files: List[Path], config: Config) -> List[Dict[str, Any]]:
    worker_args: List[Dict[str, Any]] = []
    for xlsx_file_path in separated_files:
        relative_path = xlsx_file_path.relative_to(Path(config.xlsx_output_dir))
        kml_file_rel_path = relative_path.with_suffix('.kml')
        kml_file_abs_path = Path(config.kml_output_dir) / kml_file_rel_path

        worker_args.append({
            'xlsx_file_path': str(xlsx_file_path),
            'kml_file_path': str(kml_file_abs_path),
            'xlsx_output_dir': config.xlsx_output_dir,
            'kml_output_dir': config.kml_output_dir
        })
    return worker_args


def _determine_max_workers(separated_files: List[Path], config: Config) -> int:
    if config.max_parallel_workers is not None:
        return min(len(separated_files), config.max_parallel_workers)
    else:
        return min(len(separated_files), multiprocessing.cpu_count())


def _report_conversion_results(separated_files: List[Path], conversion_errors: int, config: Config) -> None:
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

        log_file_path = "неизвестен"
        logger_root = logging.getLogger()
        if logger_root.handlers:
            for handler in logger_root.handlers:
                if hasattr(handler, 'baseFilename'):
                    log_file_path = str(getattr(handler, 'baseFilename', 'неизвестен'))
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


def process_mode_2_single_file(config: Config) -> None:
    console.print(Panel(
        "[bold cyan]Режим: Преобразование одного файла .xlsx в .kml[/bold cyan]\n\n"
        "[dim]Быстрое преобразование одного файла Excel в формат KML\n"
        "без разделения по регионам.[/dim]",
        title="🚀 Быстрое преобразование",
        border_style="cyan"
    ))

    file_name = choose_file(config)
    if not file_name:
        return

    input_path = Path(file_name)
    Path(config.single_kml_output_dir).mkdir(parents=True, exist_ok=True)
    output_filename = Path(config.single_kml_output_dir) / f"{input_path.stem}.kml"

    from rich.table import Table

    info_table = Table(show_header=False, box=None)
    info_table.add_column("Параметр", style="bold", width=20)
    info_table.add_column("Значение", style="green")
    info_table.add_row("Входной файл:", input_path.name)
    info_table.add_row("Выходной файл:", str(output_filename))

    console.print(Panel(info_table, title="ℹ️ Параметры преобразования", border_style="blue"))

    from openpyxl import load_workbook
    from src.xlsx_to_kml import create_kml_from_coordinates
    from src.stats import ProcessingStats, display_processing_statistics

    try:
        single_stats = ProcessingStats()
        single_stats.regions_detected = 1

        with console.status("[cyan]Преобразование файла в KML...[/cyan]", spinner="dots"):
            workbook = load_workbook(filename=str(input_path), data_only=True)
            conversion_result = create_kml_from_coordinates(
                workbook.active,
                output_file=str(output_filename),
                filename=input_path.name
            )

            single_stats.add_file_result(conversion_result)
            if conversion_result.anomaly_file_created:
                single_stats.anomaly_files_generated += 1

        success_msg = f"[bold green]✅ Преобразование завершено успешно![/bold green]\n\n"
        success_msg += f"Входной файл: [cyan]{input_path.name}[/cyan]\n"
        success_msg += f"Выходной файл: [blue]{output_filename}[/blue]"

        if conversion_result.anomaly_file_created:
            success_msg += f"\n\n[yellow]📊 Создан файл с аномалиями[/yellow]"

        console.print(Panel(success_msg, title="🎉 Готово", border_style="green"))

        display_processing_statistics(single_stats)

    except Exception as e:
        console.print(Panel(
            f"[bold red]Ошибка при обработке файла:[/bold red]\n{e}\n\n"
            "[dim]Проверьте, что файл не поврежден и содержит корректные данные.[/dim]",
            title="❌ Ошибка преобразования",
            border_style="red"
        ))
        logger.exception(f"Ошибка в режиме 2 при обработке файла {file_name}")


