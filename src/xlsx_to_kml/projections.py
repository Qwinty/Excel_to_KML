import json
import logging
import re
from functools import lru_cache
from typing import Dict

from pyproj import CRS, Transformer

logger = logging.getLogger(__name__)


def create_transformer(proj4_str: str) -> Transformer:
    """Создает трансформер из заданной строки Proj4 в WGS84."""
    crs = CRS.from_proj4(proj4_str)
    return Transformer.from_crs(crs, "EPSG:4326", always_xy=True)


@lru_cache(maxsize=None)
def get_transformers(proj4_path: str = "data/proj4.json") -> Dict[str, Transformer]:
    """Лениво загружает и кэширует словарь трансформеров из файла proj4.json."""
    try:
        with open(proj4_path, "r", encoding="utf-8") as f:
            proj4_strings: dict[str, str] = json.load(f)

        # Автосоздание алиасов для МСК, где есть ровно одна зона: "МСК-06 зона 1" -> "МСК-06"
        try:
            zone_key_regex = re.compile(r'^(МСК-[^з]+?)\s+зона\s+\d+\b')
            msk_groups: dict[str, list[str]] = {}

            for name in list(proj4_strings.keys()):
                match = zone_key_regex.match(name)
                if match:
                    prefix = match.group(1).strip()
                    msk_groups.setdefault(prefix, []).append(name)

            for prefix, full_names in msk_groups.items():
                if len(full_names) == 1:
                    alias_key = prefix
                    full_key = full_names[0]
                    if alias_key not in proj4_strings:
                        proj4_strings[alias_key] = proj4_strings[full_key]
                        logger.debug(f"Добавлен алиас проекции: '{alias_key}' -> '{full_key}'")
        except Exception as e:
            logger.warning(f"Не удалось создать алиасы МСК без 'зона': {e}")

        transformers: dict[str, Transformer] = {
            name: create_transformer(proj4)
            for name, proj4 in proj4_strings.items()
        }
        return transformers

    except FileNotFoundError:
        logger.critical(
            f"Critical Error: Could not find '{proj4_path}'. This file is required for coordinate transformations. Ensure it exists.")
        print(
            f"[bold red]Критическая ошибка: Не найден файл '{proj4_path}'.[/bold red]")
        print("[bold red]Этот файл необходим для преобразования координат. Убедитесь, что он находится в папке 'data' рядом с программой.[/bold red]")
        raise
    except json.JSONDecodeError:
        logger.critical(
            f"Critical Error: Could not parse '{proj4_path}'. Check the file format.")
        print(
            f"[bold red]Критическая ошибка: Не удалось прочитать файл '{proj4_path}'. Проверьте формат файла.[/bold red]")
        raise
    except Exception as e:
        logger.critical(
            f"Critical Error: An unexpected error occurred while loading projection data: {e}", exc_info=True)
        print(
            f"[bold red]Критическая ошибка: Непредвиденная ошибка при загрузке данных проекций: {e}[/bold red]")
        raise


