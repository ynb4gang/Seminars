# Iter 05: Retrieval

Ориентир: [app/retriever.py](../../app/retriever.py), [scripts/check_retrieval.py](../../scripts/check_retrieval.py)

## Что сделать

- Реализовать поиск top-k чанков по cosine similarity.
- Возвращать `text`, `doc_id`, `score` (и `name` как метаданные).

## Какие файлы

- `app/retriever.py`

## Как проверить

```bash
uv run python scripts/check_retrieval.py
```

## Ожидаемый результат

- Для релевантного запроса есть score > 0.
- Для нерелевантного запроса результаты могут быть, но со score = 0.

Дальше: [iter_06_demo_answer.md](iter_06_demo_answer.md)
