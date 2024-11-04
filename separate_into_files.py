import pandas as pd
import os

def split_excel_by_region_with_header(input_file, output_dir):
    """
    Разделяет Excel-файл с водными объектами по регионам на отдельные файлы,
    добавляя заголовок из исходного файла.

    Args:
        input_file (str): Путь к входному Excel-файлу.
        output_dir (str): Путь к директории, куда будут сохранены выходные файлы.
    """

    try:
        df = pd.read_excel(input_file, header=None)
    except FileNotFoundError:
        print(f"Ошибка: Файл {input_file} не найден.")
        return
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        return

    os.makedirs(output_dir, exist_ok=True)

    header_rows = df.iloc[3:6] # Заголовок (4 и 5 ряд)

    start_row = None
    current_region = None

    for i, row in df.iterrows():
        if i < 6:  # Пропускаем заголовок при поиске регионов
            continue

        row_value = row.iloc[0]
        if pd.notna(row_value) and isinstance(row_value, str) and "область" in row_value:  # Начало региона (теперь ищем "область")
            if start_row is not None:
                region_df = df.iloc[start_row:i]
                region_df = pd.concat([header_rows, region_df], ignore_index=True) # Добавляем заголовок
                filename = os.path.join(output_dir, f"{current_region.strip().replace(' ', '_')}.xlsx")
                region_df.to_excel(filename, index=False, header=False)

            start_row = i
            current_region = row_value  # Имя региона

        elif pd.isna(row_value) and start_row is not None and i > 7:  # Определяем конец региона по пустой ячейке, но не сразу после начала
             # Обработка последнего региона (если нет пустой строки в конце)
            region_df = df.iloc[start_row:i]
            region_df = pd.concat([header_rows, region_df], ignore_index=True)
            filename = os.path.join(output_dir, f"{current_region.strip().replace(' ', '_')}.xlsx")
            region_df.to_excel(filename, index=False, header=False)
            start_row = None


# Пример использования
input_excel_file = "input.xlsx"
output_directory = "regions_with_header"

split_excel_by_region_with_header(input_excel_file, output_directory)
print("Разделение файла завершено.")