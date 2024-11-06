import openpyxl.styles
import pandas as pd
import os
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from datetime import datetime
import json
import shutil
from utils import setup_logging




class WaterObjectsProcessor:
    def __init__(self, input_file, output_dir):
        self.input_file = input_file
        self.output_dir = output_dir
        self.history_file = "input/processing_history.json"
        self.changes_dir = "output/regions_with_header/changes_history"
        self.logger = setup_logging()

    def get_column_widths(self, input_file):
        """
        Получает ширину столбцов из исходного файла.
        """
        self.logger.info(f"Получение ширины столбцов из файла: {input_file}")
        try:
            wb = load_workbook(input_file)
            ws = wb.active
            column_widths = []
            for i in range(1, ws.max_column + 1):
                column_letter = get_column_letter(i)
                width = ws.column_dimensions[column_letter].width
                column_widths.append(width)
            self.logger.debug(f"Получены ширины столбцов: {column_widths}")
            return column_widths
        except Exception as e:
            self.logger.error(f"Ошибка при получении ширины столбцов: {e}")
            return None

    def apply_formatting(self, filename, column_widths):
        """
        Применяет форматирование к файлу.

        Args:
            filename (str): Путь к Excel-файлу
            column_widths (list): Список значений ширины столбцов
        """
        self.logger.info(f"Применение форматирования к файлу: {filename}")
        try:
            wb = load_workbook(filename)
            ws = wb.active

            # Применяем ширину столбцов
            for i, width in enumerate(column_widths, 1):
                if width is not None:
                    column_letter = get_column_letter(i)
                    ws.column_dimensions[column_letter].width = width

            # Объединяем B1 и C1
            ws.merge_cells('B1:C1')
            self.logger.debug("Объединены ячейки B1:C1")

            # Объединяем ячейки в столбцах A, D, E, F, G
            columns_to_merge = ['A', 'D', 'E', 'F', 'G']
            for col in columns_to_merge:
                ws.merge_cells(f'{col}1:{col}2')
            self.logger.debug(f"Объединены ячейки в столбцах: {columns_to_merge}")

            # Объединяем ячейки в строке 3
            ws.merge_cells('A3:G3')
            self.logger.debug("Объединены ячейки A3:G3")

            # Устанавливаем перенос текста
            for row in ws.iter_rows(min_row=1, max_row=ws.max_row, min_col=1, max_col=7):
                for cell in row:
                    cell.alignment = openpyxl.styles.Alignment(wrap_text=True)

            # Центрируем текст
            merged_cell = ws['A3']
            merged_cell.alignment = openpyxl.styles.Alignment(
                horizontal='center',
                vertical='center',
                wrap_text=True
            )

            merged_cell = ws['B1']
            merged_cell.alignment = openpyxl.styles.Alignment(
                horizontal='center',
                vertical='center',
                wrap_text=True
            )

            wb.save(filename)
            self.logger.info(f"Форматирование успешно применено к файлу: {filename}")
        except Exception as e:
            self.logger.error(f"Ошибка при применении форматирования к файлу {filename}: {e}")

    def load_processing_history(self):
        """Загружает историю обработки файлов"""
        self.logger.info("Загрузка истории обработки")
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    history = json.load(f)
                self.logger.info(f"Загружена история обработки: {len(history)} записей")
                return history
            except Exception as e:
                self.logger.error(f"Ошибка при загрузке истории: {e}")
                return {}
        self.logger.info("История обработки не найдена, создается новая")
        return {}

    def save_processing_history(self, history):
        """Сохраняет историю обработки файлов"""
        self.logger.info("Сохранение истории обработки")
        try:
            with open(self.history_file, 'w', encoding='utf-8') as f:
                json.dump(history, f, ensure_ascii=False, indent=4)
            self.logger.info("История обработки успешно сохранена")
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении истории: {e}")

    def get_regions_data(self, df):
        """
        Извлекает данные по регионам из DataFrame
        Returns:
            dict: {region_name: region_df}
        """
        regions_data = {}
        header_rows = df.iloc[3:5]

        start_row = None
        current_region = None

        for i, row in df.iterrows():
            if i < 6:
                continue

            row_value = row.iloc[0]
            if pd.notna(row_value) and isinstance(row_value, str) and "область" in row_value:
                if start_row is not None:
                    region_df = df.iloc[start_row:i]
                    region_df = pd.concat([header_rows, region_df], ignore_index=True)
                    regions_data[current_region] = region_df

                start_row = i
                current_region = row_value.strip()

            elif pd.isna(row_value) and start_row is not None and i > 7:
                region_df = df.iloc[start_row:i]
                region_df = pd.concat([header_rows, region_df], ignore_index=True)
                regions_data[current_region] = region_df
                start_row = None

        if start_row is not None:
            region_df = df.iloc[start_row:]
            region_df = pd.concat([header_rows, region_df], ignore_index=True)
            regions_data[current_region] = region_df

        return regions_data

    def compare_regions(self, current_df, previous_df):
        """
        Сравнивает данные водных объектов в регионе между версиями

        Args:
            current_df: DataFrame текущей версии региона
            previous_df: DataFrame предыдущей версии региона

        Returns:
            tuple: (has_changes, new_objects, removed_objects, modified_objects)
        """
        try:
            # Пропускаем заголовочные строки и берем только данные объектов
            current_data = current_df.iloc[2:]
            previous_data = previous_df.iloc[2:]

            # Создаем уникальные идентификаторы для водных объектов
            # Используем комбинацию названия, местоположения и цели использования
            def create_object_id(row):
                return tuple(str(x).strip() for x in row.iloc[[1, 2, 3]])  # колонки B, C, D

            current_objects = {create_object_id(row): row for _, row in current_data.iterrows()}
            previous_objects = {create_object_id(row): row for _, row in previous_data.iterrows()}

            current_ids = set(current_objects.keys())
            previous_ids = set(previous_objects.keys())

            # Находим новые объекты
            new_objects = []
            for obj_id in (current_ids - previous_ids):
                new_objects.append({
                    'id': obj_id,
                    'data': current_objects[obj_id].to_dict()
                })

            # Находим удаленные объекты
            removed_objects = []
            for obj_id in (previous_ids - current_ids):
                removed_objects.append({
                    'id': obj_id,
                    'data': previous_objects[obj_id].to_dict()
                })

            # Находим измененные объекты
            modified_objects = []
            for obj_id in (current_ids & previous_ids):
                current_obj = current_objects[obj_id]
                previous_obj = previous_objects[obj_id]

                # Сравниваем все значения в строках
                if not current_obj.equals(previous_obj):
                    modified_objects.append({
                        'id': obj_id,
                        'old_data': previous_obj.to_dict(),
                        'new_data': current_obj.to_dict()
                    })

            has_changes = bool(new_objects or removed_objects or modified_objects)
            return has_changes, new_objects, removed_objects, modified_objects

        except Exception as e:
            self.logger.error(f"Ошибка при сравнении данных региона: {e}")
            return False, [], [], []

    def save_changes_report(self, region_name, new_objects, removed_objects, modified_objects):
        """
        Сохраняет подробный отчет об изменениях водных объектов
        """
        os.makedirs(self.changes_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = os.path.join(self.changes_dir, f"{region_name}_changes_{timestamp}.txt")

        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"Отчет об изменениях водных объектов для региона: {region_name}\n")
            f.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Новые объекты
            f.write(f"Новые водные объекты ({len(new_objects)}):\n")
            for obj in new_objects:
                f.write("-" * 50 + "\n")
                f.write(f"Название: {obj['data'].get(1, 'Н/Д')}\n")
                f.write(f"Местоположение: {obj['data'].get(2, 'Н/Д')}\n")
                f.write(f"Цель использования: {obj['data'].get(3, 'Н/Д')}\n")

            # Удаленные объекты
            f.write(f"\nУдаленные водные объекты ({len(removed_objects)}):\n")
            for obj in removed_objects:
                f.write("-" * 50 + "\n")
                f.write(f"Название: {obj['data'].get(1, 'Н/Д')}\n")
                f.write(f"Местоположение: {obj['data'].get(2, 'Н/Д')}\n")
                f.write(f"Цель использования: {obj['data'].get(3, 'Н/Д')}\n")

            # Измененные объекты
            f.write(f"\nИзмененные водные объекты ({len(modified_objects)}):\n")
            for obj in modified_objects:
                f.write("-" * 50 + "\n")
                f.write(f"Водный объект: {obj['id'][0]}\n")
                f.write("Изменения:\n")

                old_data = obj['old_data']
                new_data = obj['new_data']

                # Сравниваем значения по всем колонкам
                for col in old_data.keys():
                    old_val = str(old_data[col]).strip()
                    new_val = str(new_data[col]).strip()
                    if old_val != new_val:
                        f.write(f"Колонка {col}:\n")
                        f.write(f"  Было:  {old_val}\n")
                        f.write(f"  Стало: {new_val}\n")

    def process_file(self):
        """Основной метод обработки файла"""
        self.logger.info(f"Начало обработки файла: {self.input_file}")
        history = self.load_processing_history()

        try:
            # Получаем ширину столбцов из исходного файла
            column_widths = self.get_column_widths(self.input_file)
            if column_widths is None:
                self.logger.warning("Не удалось получить ширину столбцов, продолжаем без форматирования")

            # Загружаем текущий файл
            current_df = pd.read_excel(self.input_file, header=None)
            self.logger.info(f"Загружен текущий файл. Размер: {current_df.shape}")

            # Получаем данные по регионам из текущего файла
            current_regions = self.get_regions_data(current_df)
            self.logger.info(f"Найдено регионов: {len(current_regions)}")

            # Загружаем предыдущую версию файла
            previous_file = history.get('latest_file')
            if previous_file and os.path.exists(previous_file):
                previous_df = pd.read_excel(previous_file, header=None)
                previous_regions = self.get_regions_data(previous_df)
                self.logger.info("Загружена предыдущая версия файла")
            else:
                previous_regions = {}
                self.logger.info("Предыдущая версия файла не найдена")

            # Создаем директорию для выходных файлов
            os.makedirs(self.output_dir, exist_ok=True)

            # Сравниваем регионы и сохраняем только измененные
            changed_regions = []
            for region_name, current_region_df in current_regions.items():
                if region_name in previous_regions:
                    has_changes, new_objects, removed_objects, modified_objects = self.compare_regions(
                        current_region_df, previous_regions[region_name]
                    )

                    if has_changes:
                        self.logger.info(f"Обнаружены изменения в регионе: {region_name}")
                        changed_regions.append(region_name)

                        # Сохраняем файл региона
                        output_file = os.path.join(self.output_dir,
                                                   f"{region_name.strip().replace(' ', '_')}.xlsx")
                        current_region_df.to_excel(output_file, index=False, header=False)

                        # Применяем форматирование
                        if column_widths is not None:
                            self.apply_formatting(output_file, column_widths)

                        # Сохраняем отчет об изменениях
                        self.save_changes_report(region_name, new_objects, removed_objects, modified_objects)
                else:
                    # Новый регион
                    self.logger.info(f"Обнаружен новый регион: {region_name}")
                    changed_regions.append(region_name)
                    output_file = os.path.join(self.output_dir,
                                               f"{region_name.strip().replace(' ', '_')}.xlsx")
                    current_region_df.to_excel(output_file, index=False, header=False)

                    # Применяем форматирование
                    if column_widths is not None:
                        self.apply_formatting(output_file, column_widths)

            # Обновляем историю
            history['latest_file'] = self.input_file
            history['last_processed'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            history['changed_regions'] = changed_regions

            # Сохраняем копию текущего файла
            history_dir = "input/history"  # Изменен путь для сохранения истории
            os.makedirs(history_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            history_file = os.path.join(history_dir, f"water_objects_{timestamp}.xlsx")
            shutil.copy2(self.input_file, history_file)

            self.save_processing_history(history)

            self.logger.info(f"Обработка завершена. Изменения найдены в {len(changed_regions)} регионах")
            if changed_regions:
                self.logger.info("Измененные регионы:")
                for region in changed_regions:
                    self.logger.info(f"- {region}")

        except Exception as e:
            self.logger.error(f"Ошибка при обработке файла: {e}")
            raise


# Пример использования
if __name__ == "__main__":
    input_excel_file = "input/Forma-informatsii-o-predostavlenii-vodnykh-obektov-v-polzovanie20241031.xlsx"
    output_directory = "output/separated_regions"

    processor = WaterObjectsProcessor(input_excel_file, output_directory)
    processor.process_file()
