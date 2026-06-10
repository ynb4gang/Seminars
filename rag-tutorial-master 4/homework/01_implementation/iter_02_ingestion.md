# Iter 02: Ingestion

Ориентир: [scripts/ingest.py](../../scripts/ingest.py)

## Что сделать

- Преобразовать `datasets.json` в `documents.jsonl`.
- Добавить метаданные `doc_id`, `name`, `source_file`.

## Какие файлы

- `scripts/ingest.py`
- `data/processed/documents.jsonl` (генерируется)

## Как проверить

```bash
uv run python scripts/ingest.py
```

## Ожидаемый результат

- Создан `data/processed/documents.jsonl`.
- Количество строк соответствует числу записей в `datasets.json`.

Дальше: [iter_03_chunking.md](iter_03_chunking.md)
