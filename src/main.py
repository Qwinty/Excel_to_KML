import logging
import multiprocessing
from rich import traceback

from src.utils import setup_logging
from src.config import Config
from src.ui import console, display_welcome, show_main_menu
from src.processing import process_mode_1_full_processing, process_mode_2_single_file
from src.debug_parser import debug_coordinate_parser


# Better tracebacks
traceback.install(show_locals=True)


def main() -> None:
    """Main application entry point."""
    # Configure logging once for the main process
    setup_logging(console_level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    config = Config()

    display_welcome()

    while True:
        try:
            user_input = show_main_menu()
        except (KeyboardInterrupt, EOFError):
            logger.info("Program terminated by user.")
            console.print("\n[yellow]Работа программы завершена.[/yellow]")
            break

        if user_input == "1":
            process_mode_1_full_processing(config)
        elif user_input == "2":
            process_mode_2_single_file(config)
        elif user_input == "3":
            debug_coordinate_parser()
        elif user_input == "4":
            from rich.panel import Panel

            console.print(Panel(
                "[yellow]Спасибо за использование Excel to KML Converter![/yellow]\n\n"
                "[dim]Программа завершена.[/dim]",
                title="👋 До свидания",
                border_style="yellow"
            ))
            break


if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()


