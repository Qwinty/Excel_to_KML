import logging
from typing import Any, List, Optional, Tuple

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table

from src.utils import setup_logging
from src.xlsx_to_kml import parse_coordinates, process_coordinates, create_transformer


console = Console()
logger = logging.getLogger(__name__)


def _setup_debug_logging():
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

    return console_handler, original_console_level


def _cleanup_debug_logging(console_handler: Optional[logging.Handler], original_console_level: Optional[int]):
    if console_handler and original_console_level is not None:
        logger.debug(
            f"Восстановлен исходный уровень логирования консоли: {logging.getLevelName(original_console_level)}")
        console_handler.setLevel(original_console_level)


def _get_debug_mode_choice() -> str:
    mode_table = Table(title="Режимы парсинга", show_header=False, box=None)
    mode_table.add_column("№", style="bold cyan", width=3)
    mode_table.add_column("Описание", style="white")

    mode_table.add_row("1", "Автоматический режим (как в основной программе)")
    mode_table.add_row("2", "Ввести собственную proj4 строку")
    mode_table.add_row("3", "Вернуться в главное меню")

    console.print(mode_table)

    try:
        return Prompt.ask(
            "Введите номер режима",
            choices=["1", "2", "3"],
            show_choices=False
        )
    except (KeyboardInterrupt, EOFError):
        console.print("\n[yellow]Ввод отменен. Возврат в главное меню.[/yellow]")
        return "3"


def _get_custom_proj4_transformer() -> Tuple[Optional[Any], Optional[str]]:
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
                return None, None

            if not custom_proj4.startswith('+proj'):
                console.print(
                    "[yellow]Proj4 строка должна начинаться с '+proj'.[/yellow]")
                continue

            selected_transformer = create_transformer(custom_proj4)
            selected_proj4_name = "Пользовательская proj4"

            display_proj4 = custom_proj4[:60] + "..." if len(custom_proj4) > 60 else custom_proj4
            console.print(
                f"[green]✓ Proj4 строка успешно загружена:[/green] [dim]{display_proj4}[/dim]")
            return selected_transformer, selected_proj4_name

        except (KeyboardInterrupt, EOFError):
            console.print("\n[yellow]Ввод отменен.[/yellow]")
            return None, None
        except Exception as e:
            console.print(Panel(
                f"[bold red]Ошибка при загрузке proj4 строки:[/bold red]\n{e}\n\n"
                "[yellow]Попробуйте ввести корректную proj4 строку или введите 'back' для возврата.[/yellow]",
                title="Ошибка",
                border_style="red"
            ))
            continue


def _parse_coordinate_string(input_string: str, mode_choice: str, selected_transformer: Optional[Any]):
    logger.info(f"--- Начало парсинга строки: '{input_string}' ---")

    if mode_choice == "1":
        return parse_coordinates(input_string)
    elif mode_choice == "2":
        if (' м.' in input_string or ', м.' in input_string or input_string.endswith('м.')) and '°' not in input_string:
            return process_coordinates(input_string, selected_transformer)
        else:
            return parse_coordinates(input_string)

    return None, "Неизвестный режим парсинга"


def _display_parsing_results(coords, reason):
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
        result_table = Table(title=f"✅ Найдено {len(coords)} координат", show_header=True, header_style="bold green")
        result_table.add_column("№", style="dim", width=3, justify="center")
        result_table.add_column("Имя", style="cyan")
        result_table.add_column("Долгота", style="green", justify="right")
        result_table.add_column("Широта", style="green", justify="right")

        for i, (name, lon, lat) in enumerate(coords, 1):
            result_table.add_row(str(i), name, f"{lon:.6f}", f"{lat:.6f}")

        console.print(result_table)
        console.print("\n[bold blue]📍 Формат для Geobridge:[/bold blue]")
        for name, lon, lat in coords:
            console.print(f"{lat}, {lon}")

    console.print()


def _run_coordinate_parsing_loop(mode_choice: str, selected_transformer, selected_proj4_name):
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

            coords, reason = _parse_coordinate_string(input_string, mode_choice, selected_transformer)
            _display_parsing_results(coords, reason)

        except (KeyboardInterrupt, EOFError):
            console.print("\n[yellow]Ввод отменен.[/yellow]")
            break


def debug_coordinate_parser() -> None:
    console.print(Panel(
        "[bold magenta]Режим отладки парсера координат[/bold magenta]\n"
        "[dim]Введите строки для парсинга координат и тестирования различных proj4 систем[/dim]",
        title="🔧 Отладка",
        border_style="magenta"
    ))

    console_handler, original_console_level = _setup_debug_logging()

    try:
        while True:
            mode_choice = _get_debug_mode_choice()

            if mode_choice == "3":
                break

            selected_transformer = None
            selected_proj4_name = None

            if mode_choice == "2":
                selected_transformer, selected_proj4_name = _get_custom_proj4_transformer()
                if not selected_transformer:
                    continue

            _run_coordinate_parsing_loop(mode_choice, selected_transformer, selected_proj4_name)

    except (KeyboardInterrupt, EOFError):
        console.print("\n[yellow]Выход из режима отладки. Возврат в главное меню.[/yellow]")
    finally:
        _cleanup_debug_logging(console_handler, original_console_level)


