import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.xlsx_to_kml import ConversionResult

console = Console()


@dataclass
class ProcessingStats:
    """Aggregate statistics for the entire processing session."""
    start_time: float = field(default_factory=time.time)
    regions_detected: int = 0
    files_created: List[str] = field(default_factory=list)
    file_results: Dict[str, ConversionResult] = field(default_factory=dict)
    conversion_errors: int = 0
    anomaly_files_generated: int = 0

    def add_file_result(self, result: ConversionResult) -> None:
        self.file_results[result.filename] = result

    def get_processing_time(self) -> float:
        return time.time() - self.start_time

    def get_total_stats(self) -> Dict[str, int]:
        totals = {
            'total_files': len(self.file_results),
            'total_rows': 0,
            'successful_rows': 0,
            'failed_rows': 0,
            'anomaly_rows': 0
        }

        for result in self.file_results.values():
            totals['total_rows'] += result.total_rows
            totals['successful_rows'] += result.successful_rows
            totals['failed_rows'] += result.failed_rows
            totals['anomaly_rows'] += result.anomaly_rows

        return totals

    def get_most_problematic_files(self, top_n: int = 5) -> List[ConversionResult]:
        files_with_issues = [
            result for result in self.file_results.values()
            if result.total_rows > 0 and result.failure_rate > 0
        ]

        sorted_files = sorted(
            files_with_issues, key=lambda x: x.failure_rate, reverse=True)
        return sorted_files[:top_n]

    def calculate_quality_score(self) -> Dict[str, Any]:
        totals = self.get_total_stats()

        if totals['total_rows'] == 0:
            return {'parsing': 0, 'completeness': 0, 'consistency': 0, 'overall': 0}

        parsing_score = (totals['successful_rows'] /
                         totals['total_rows']) * 100

        completeness_score = max(
            0, 100 - (totals['failed_rows'] / totals['total_rows']) * 50)

        all_errors: List[str] = []
        for result in self.file_results.values():
            all_errors.extend(result.error_reasons)

        unique_errors = len(set(all_errors)) if all_errors else 0

        error_analysis = None
        if all_errors:
            from collections import Counter
            import re

            grouped_errors: Counter[str] = Counter()
            error_patterns = {
                r'Нечетное количество найденных ДМС координат \(\d+\)': 'Нечетное количество найденных ДМС координат',
                r'Нечетное количество найденных ЛМС координат \(\d+\)': 'Нечетное количество найденных ЛМС координат',
                r'Координаты ДМС вне допустимого диапазона WGS84 \(lat=[-\d.]+, lon=[-\d.]+\)': 'Координаты ДМС вне допустимого диапазона WGS84',
                r'Координаты МСК вне допустимого диапазона WGS84 \(lat=[-\d.]+, lon=[-\d.]+\)': 'Координаты МСК вне допустимого диапазона WGS84',
                r'Ошибка трансформации МСК координат: .+': 'Ошибка трансформации МСК координат',
                r'Обнаружены аномальные координаты, значительно удаленные от других': 'Обнаружены аномальные координаты, значительно удаленные от других'
            }

            for error in all_errors:
                grouped = False
                for pattern, group_name in error_patterns.items():
                    if re.match(pattern, error):
                        grouped_errors[group_name] += 1
                        grouped = True
                        break
                if not grouped:
                    display_error = error[:80] + "..." if len(error) > 80 else error
                    grouped_errors[display_error] += 1

            error_analysis = {
                'total_errors': len(all_errors),
                'unique_types': len(grouped_errors),
                'top_errors': grouped_errors.most_common(10)
            }

        consistency_score = max(20, 100 - (unique_errors * 2))

        overall = (parsing_score * 0.5 + completeness_score *
                   0.3 + consistency_score * 0.2)

        return {
            'parsing': round(parsing_score, 1),
            'completeness': round(completeness_score, 1),
            'consistency': round(consistency_score, 1),
            'overall': round(overall, 1),
            'error_analysis': error_analysis
        }


def display_error_analysis(error_analysis: Dict[str, Any]) -> None:
    if not error_analysis or not error_analysis.get('top_errors'):
        return

    error_table = Table(show_header=True, header_style="bold yellow")
    error_table.add_column("№", width=3, justify="center")
    error_table.add_column("Тип ошибки", min_width=40)
    error_table.add_column("Количество", justify="right", style="red")
    error_table.add_column("Процент", justify="right", style="bright_yellow")

    total_errors = error_analysis['total_errors']

    for i, (error_type, count) in enumerate(error_analysis['top_errors'], 1):
        percentage = (count / total_errors) * 100
        display_error = error_type[:60] + "..." if len(error_type) > 60 else error_type
        error_table.add_row(str(i), display_error, str(count), f"{percentage:.1f}%")

    if len(error_analysis['top_errors']) < error_analysis['unique_types']:
        remaining_types = error_analysis['unique_types'] - len(error_analysis['top_errors'])
        remaining_count = total_errors - sum(count for _, count in error_analysis['top_errors'])
        remaining_percentage = (remaining_count / total_errors) * 100 if total_errors > 0 else 0

        error_table.add_row(
            "...",
            f"Другие типы ошибок ({remaining_types} типов)",
            str(remaining_count),
            f"{remaining_percentage:.1f}%",
            style="dim"
        )

    console.print(Panel(
        error_table,
        title=f"🔍 Анализ ошибок ({error_analysis['unique_types']} уникальных типов, {total_errors} всего)",
        border_style="yellow"
    ))


def _format_processing_time(processing_time: float) -> str:
    if processing_time < 60:
        return f"{processing_time:.1f}с"
    else:
        minutes = int(processing_time // 60)
        seconds = int(processing_time % 60)
        return f"{minutes}м {seconds}с"


def _create_progress_bar(value: float, width: int = 20) -> str:
    filled = int(value / 5)
    empty = width - filled
    return "█" * filled + "▌" * (1 if value % 5 >= 2.5 else 0) + "░" * (empty - (1 if value % 5 >= 2.5 else 0))


def _get_quality_grade_and_color(overall_score: float):
    if overall_score >= 90:
        return "green", "Отлично"
    elif overall_score >= 80:
        return "bright_green", "Хорошо"
    elif overall_score >= 70:
        return "yellow", "Удовлетворительно"
    elif overall_score >= 60:
        return "bright_red", "Плохо"
    else:
        return "red", "Очень плохо"


def _display_processing_summary(stats: ProcessingStats, totals: Dict[str, int], time_str: str) -> None:
    success_rate = (totals['successful_rows'] / totals['total_rows'] * 100) if totals['total_rows'] > 0 else 0

    summary_table = Table(show_header=False, box=None, padding=(0, 1))
    summary_table.add_column("Параметр", style="bold", width=25)
    summary_table.add_column("Значение", style="green")

    summary_table.add_row("Файлов обнаружено:", f"{stats.regions_detected} регионов")
    if stats.anomaly_files_generated > 0:
        summary_table.add_row("Файлы с аномалиями:", f"{stats.anomaly_files_generated} файла")
    summary_table.add_row("Объектов обработано:", f"{totals['total_rows']} строк → {totals['successful_rows']} успешно ({success_rate:.1f}%)")
    summary_table.add_row("Время обработки:", time_str)

    console.print(Panel(
        summary_table,
        title="📊 Сводка обработки",
        border_style="cyan"
    ))


def _display_problematic_files(stats: ProcessingStats) -> None:
    problematic_files = stats.get_most_problematic_files(7)
    if not problematic_files:
        return

    problem_table = Table(show_header=True, header_style="bold red")
    problem_table.add_column("№", width=3, justify="center")
    problem_table.add_column("Файл", min_width=30)
    problem_table.add_column("Проблемные строки", justify="right", style="red")
    problem_table.add_column("Процент", justify="right", style="yellow")

    for i, result in enumerate(problematic_files, 1):
        problem_table.add_row(
            str(i),
            result.filename,
            f"{result.failed_rows}/{result.total_rows} объектов",
            f"{result.failure_rate:.1f}%"
        )

    console.print(Panel(
        problem_table,
        title=f"⚠️ Наиболее проблемные файлы (топ {len(problematic_files)})",
        border_style="red"
    ))


def _display_quality_scores(quality_scores: Dict[str, Any]) -> None:
    overall_score = quality_scores['overall']
    overall_color, overall_grade = _get_quality_grade_and_color(overall_score)

    console.print(Panel(
        f"[bold {overall_color}]Общая оценка качества: {overall_score:.0f}/100 ({overall_grade})[/bold {overall_color}]\n\n"
        f"• Парсинг координат: {quality_scores['parsing']:.1f}% {_create_progress_bar(quality_scores['parsing'])} ({quality_scores['parsing']:.0f}/100)\n"
        f"• Полнота данных: {quality_scores['completeness']:.1f}% {_create_progress_bar(quality_scores['completeness'])} ({quality_scores['completeness']:.0f}/100)\n"
        f"• Согласованность форматов: {quality_scores['consistency']:.1f}% {_create_progress_bar(quality_scores['consistency'])} ({quality_scores['consistency']:.0f}/100)",
        title="🎯 Оценка качества данных",
        border_style="blue"
    ))


def _display_additional_info(stats: ProcessingStats) -> None:
    if stats.conversion_errors > 0:
        console.print(
            f"[yellow]⚠️ Дополнительно: {stats.conversion_errors} файлов не удалось обработать из-за критических ошибок.[/yellow]")


def display_processing_statistics(stats: ProcessingStats) -> None:
    if not stats.file_results:
        console.print("[yellow]Нет данных для отображения статистики.[/yellow]")
        return

    totals = stats.get_total_stats()
    processing_time = stats.get_processing_time()
    quality_scores = stats.calculate_quality_score()
    time_str = _format_processing_time(processing_time)

    _display_processing_summary(stats, totals, time_str)
    _display_problematic_files(stats)
    _display_quality_scores(quality_scores)

    if quality_scores.get('error_analysis'):
        display_error_analysis(quality_scores['error_analysis'])

    _display_additional_info(stats)
    console.print()


