import argparse
import sqlite3
import pickle
import hashlib
import os
import sys


def compute_hash(data: bytes) -> str:
    """Вычисляет SHA256 хэш от данных изображения."""
    return hashlib.sha256(data).hexdigest()


def find_duplicates(db_file: str, output_dir: str = '.'):
    """
    Находит дубликаты тайлов в базе данных SQLite.
    
    Сохраняет словарь hash.pickle, где:
    - ключ: hashsum тайла
    - значение: tuple (x_first, y_first, count)
      - x_first, y_first: координаты первого тайла с таким хэшем
      - count: количество тайлов с таким хэшем
    """
    if not os.path.exists(db_file):
        print(f"Ошибка: файл базы данных '{db_file}' не найден")
        sys.exit(1)

    # Извлекаем номер zoom уровня из имени файла
    zoom = None
    base_name = os.path.basename(db_file)
    if base_name.startswith('tiles_') and base_name.endswith('.db3'):
        try:
            zoom = int(base_name[6:-4])
        except ValueError:
            pass

    if zoom is None:
        print("Не удалось определить zoom уровень из имени файла")
        zoom = 0

    table_name = f'z{zoom}'
    print(f"Обработка базы данных: {db_file}")
    print(f"Таблица: {table_name}")

    # Вычисляем ожидаемое количество тайлов: (2^zoom) × (2^zoom) = 2^(2×zoom)
    expected_total = (2 ** zoom) ** 2
    print(f"Ожидаемое количество тайлов: {expected_total}")

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    hash_dict = {}  # hashsum -> (x_first, y_first, count)

    batch_size = 100
    offset = 0
    processed = 0

    while True:
        # Получаем порцию записей
        cursor.execute(
            f"SELECT x, y, image FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
        )
        rows = cursor.fetchall()

        if not rows:
            break  # Больше нет записей

        for x, y, image_data in rows:
            hashsum = compute_hash(image_data)
            processed += 1

            # Вывод прогресса каждые 1000 тайлов
            if processed % 1000 == 0:
                percent = (processed / expected_total) * 100 if expected_total > 0 else 0
                print(f"[{percent:.2f}%] Тайл [{x}, {y}] | Обработано {processed} | Хэш: {hashsum[:16]}...")

            if hashsum in hash_dict:
                x_first, y_first, count = hash_dict[hashsum]
                hash_dict[hashsum] = (x_first, y_first, count + 1)
            else:
                hash_dict[hashsum] = (x, y, 1)

        offset += batch_size

    conn.close()

    print(f"\nВсего обработано тайлов: {processed}")

    # Сохраняем результат в pickle файл
    pickle_file = os.path.join(output_dir, 'hash.pickle')
    with open(pickle_file, 'wb') as f:
        pickle.dump(hash_dict, f)

    print(f"\nРезультат сохранён в: {pickle_file}")

    # Статистика по дубликатам
    duplicates = {h: data for h, data in hash_dict.items() if data[2] > 1}
    unique_count = len(hash_dict) - len(duplicates)
    
    print(f"\nСтатистика:")
    print(f"  Уникальных хэшей: {len(hash_dict)}")
    print(f"  Хэшей с дубликатами: {len(duplicates)}")
    print(f"  Тайлов без дубликатов: {unique_count}")
    
    if duplicates:
        total_duplicate_tiles = sum(data[2] - 1 for data in duplicates.values())
        print(f"  Всего дубликатов (избыточных тайлов): {total_duplicate_tiles}")
        
        # Топ-10 хэшей по количеству дубликатов
        sorted_dups = sorted(duplicates.items(), key=lambda x: x[1][2], reverse=True)[:10]
        print(f"\nТоп-10 хэшей по количеству дубликатов:")
        for hashsum, (x_first, y_first, count) in sorted_dups:
            print(f"  {hashsum[:16]}...: ({x_first}, {y_first}) × {count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Поиск дубликатов тайлов в базе данных SQLite')
    parser.add_argument('-d', '--db', default='tiles_10.db3', help='Файл базы данных SQLite (по умолчанию: tiles_10.db3)')
    parser.add_argument('-o', '--output-dir', default='.', help='Директория для сохранения hash.pickle (по умолчанию: текущая)')
    
    args = parser.parse_args()
    
    find_duplicates(args.db, args.output_dir)
