# Iter 04: TF-IDF Index

Ориентир: [scripts/build_index.py](../../scripts/build_index.py)

## Что сделать

- Построить индекс: ingest + chunk + TF-IDF fit.
- Сохранить артефакты индекса.

## Какие файлы

- `scripts/build_index.py`
- `data/index/vectorizer.pkl` (генерируется)
- `data/index/matrix.npz` (генерируется)
- `data/index/chunks.jsonl` (генерируется)

## Как проверить

```bash
uv run python scripts/build_index.py
```

## Ожидаемый результат

- В `data/index/` есть три файла индекса.
- В консоли виден размер матрицы (`чанки x признаки`).

Дальше: [iter_05_retrieval.md](iter_05_retrieval.md)
