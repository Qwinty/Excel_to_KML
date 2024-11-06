import openpyxl.styles
import pandas as pd
import os
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter


def get_column_widths(input_file):
    """
    Получает ширину столбцов из исходного файла.

    Args:
        input_file (str): Путь к входному Excel-файлу.

    Returns:
        list: Список значений ширины столбцов.
    """
    try:
        wb = load_workbook(input_file)
        ws = wb.active
        column_widths = []
        for i in range(1, ws.max_column + 1):
            column_letter = get_column_letter(i)
            width = ws.column_dimensions[column_letter].width
            column_widths.append(width)
        return column_widths
    except Exception as e:
        print(f"Ошибка при получении ширины столбцов: {e}")
        return None


def apply_formatting(filename, column_widths):
    """
    Применяет форматирование к файлу.

    Args:
        filename (str): Путь к Excel-файлу
        column_widths (list): Список значений ширины столбцов
    """
    try:
        wb = load_workbook(filename)
        ws = wb.active

        # Применяем ширину столбцов
        for i, width in enumerate(column_widths, 1):
            if width is not None:
                column_letter = get_column_letter(i)
                ws.column_dimensions[column_letter].width = width

        # Объединяем B1 и C1 (вместо C1:C2)
        ws.merge_cells('B1:C1')

        # Объединяем ячейки в столбцах A, D, E, F, G (строки 1 и 2)
        columns_to_merge = ['A', 'D', 'E', 'F', 'G']
        for col in columns_to_merge:
            ws.merge_cells(f'{col}1:{col}2')

        # Объединяем ячейки в строке 3 от A до G
        ws.merge_cells('A3:G3')

        # Устанавливаем перенос текста для столбцов A-G
        for row in ws.iter_rows(min_row=1, max_row=ws.max_row, min_col=1, max_col=7):
            for cell in row:
                cell.alignment = openpyxl.styles.Alignment(wrap_text=True)

        # Центрируем текст в объединенной ячейке третьей строки
        merged_cell = ws['A3']
        merged_cell.alignment = openpyxl.styles.Alignment(
            horizontal='center',
            vertical='center',
            wrap_text=True
        )

        # Центрируем текст в объединенной ячейке первой строки
        merged_cell = ws['B1']
        merged_cell.alignment = openpyxl.styles.Alignment(
            horizontal='center',
            vertical='center',
            wrap_text=True
        )

        wb.save(filename)
    except Exception as e:
        print(f"Ошибка при применении форматирования к файлу {filename}: {e}")


def split_excel_by_region_with_header(input_file, output_dir):
    """
    Разделяет Excel-файл с водными объектами по регионам на отдельные файлы,
    сохраняя форматирование.
    """
    try:
        # Получаем ширину столбцов из исходного файла
        column_widths = get_column_widths(input_file)
        if column_widths is None:
            print("Не удалось получить ширину столбцов, продолжаем без форматирования")

        df = pd.read_excel(input_file, header=None)
    except FileNotFoundError:
        print(f"Ошибка: Файл {input_file} не найден.")
        return
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        return

    os.makedirs(output_dir, exist_ok=True)

    header_rows = df.iloc[3:5]  # Заголовок (4 и 5 ряд)

    start_row = None
    current_region = None

    for i, row in df.iterrows():
        if i < 6:  # Пропускаем заголовок при поиске регионов
            continue

        row_value = row.iloc[0]
        if pd.notna(row_value) and isinstance(row_value, str) and "область" in row_value:
            if start_row is not None:
                region_df = df.iloc[start_row:i]
                region_df = pd.concat([header_rows, region_df], ignore_index=True)
                filename = os.path.join(output_dir, f"{current_region.strip().replace(' ', '_')}.xlsx")
                region_df.to_excel(filename, index=False, header=False)

                # Применяем форматирование
                if column_widths is not None:
                    apply_formatting(filename, column_widths)

            start_row = i
            current_region = row_value

        elif pd.isna(row_value) and start_row is not None and i > 7:
            region_df = df.iloc[start_row:i]
            region_df = pd.concat([header_rows, region_df], ignore_index=True)
            filename = os.path.join(output_dir, f"{current_region.strip().replace(' ', '_')}.xlsx")
            region_df.to_excel(filename, index=False, header=False)

            # Применяем форматирование
            if column_widths is not None:
                apply_formatting(filename, column_widths)

            start_row = None

    # Обработка последнего региона
    if start_row is not None:
        region_df = df.iloc[start_row:]
        region_df = pd.concat([header_rows, region_df], ignore_index=True)
        filename = os.path.join(output_dir, f"{current_region.strip().replace(' ', '_')}.xlsx")
        region_df.to_excel(filename, index=False, header=False)

        # Применяем форматирование
        if column_widths is not None:
            apply_formatting(filename, column_widths)


# Пример использования
input_excel_file = "input/Forma-informatsii-o-predostavlenii-vodnykh-obektov-v-polzovanie20241031.xlsx"
output_directory = "regions_with_header"

split_excel_by_region_with_header(input_excel_file, output_directory)
print("Разделение файла завершено.")