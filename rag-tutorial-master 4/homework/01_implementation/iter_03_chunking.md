# Iter 03: Chunking

Ориентир: [app/chunker.py](../../app/chunker.py), [tests/test_chunking.py](../../tests/test_chunking.py)

## Что сделать

- Нарезать документы на чанки по абзацам.
- Соблюсти `max_chars` и `overlap`.

## Какие файлы

- `app/chunker.py`
- `data/processed/chunks.jsonl` (генерируется)
- `tests/test_chunking.py`

## Как проверить

```bash
uv run pytest tests/test_chunking.py -v
```

## Ожидаемый результат

- Тесты chunking зеленые.
- `chunks.jsonl` создан и содержит чанки с `doc_id`.

Дальше: [iter_04_index.md](iter_04_index.md)
