# Iter 01: Demo Data

Ориентир: [doc/tasklist.md](../../doc/tasklist.md), [scripts/prepare_datasets.py](../../scripts/prepare_datasets.py)

## Что сделать

- Подготовить `data/raw/datasets.json` с текстовыми записями для RAG.
- Выбрать источник данных (желательно 1000+ в исходном датасете).

## Какие файлы

- `data/raw/datasets.json`
- опционально `scripts/prepare_datasets.py`

## Как проверить

```bash
uv run python -c "import json; d=json.load(open('data/raw/datasets.json', encoding='utf-8')); print(len(d['datasets']))"
```

## Ожидаемый результат

- JSON читается.
- Внутри есть массив `datasets` с минимум 10 записями для MVP.

Дальше: [iter_02_ingestion.md](iter_02_ingestion.md)
