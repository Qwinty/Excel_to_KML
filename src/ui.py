from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, IntPrompt

from src.config import Config


# Single console instance for the whole app
console = Console()


def display_welcome() -> None:
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

    menu_table.add_row(
        "1", "Разделить файл по регионам и преобразовать в KML", "Полный цикл обработки")
    menu_table.add_row(
        "2", "Преобразовать один файл .xlsx в .kml", "Быстрое преобразование")
    menu_table.add_row(
        "3", "Отладочный парсинг строк с координатами", "Тестирование парсера")
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


def choose_file(config: Config) -> Optional[str]:
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
    table = Table(title="Доступные файлы Excel",
                  show_header=True, header_style="bold cyan")
    table.add_column("№", style="dim", width=4, justify="center")
    table.add_column("Имя файла", min_width=20)
    table.add_column("Размер", justify="right", style="green")
    table.add_column("Дата изменения", justify="center", style="blue")

    for i, file_path in enumerate(files, 1):
        size_kb = file_path.stat().st_size / 1024
        size_str = f"{size_kb:.1f} KB" if size_kb < 1024 else f"{size_kb/1024:.1f} MB"
        mod_time = file_path.stat().st_mtime
        import datetime
        mod_date = datetime.datetime.fromtimestamp(
            mod_time).strftime("%Y-%m-%d %H:%M")

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


