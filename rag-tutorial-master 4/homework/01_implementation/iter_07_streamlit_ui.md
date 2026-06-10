# Iter 07: Streamlit UI

Ориентир: [app/main.py](../../app/main.py)

## Что сделать

- Сделать UI для вопроса, найденных фрагментов, ответа и источников.
- Показать сообщение, если индекс не собран.

## Какие файлы

- `app/main.py`

## Как проверить

```bash
uv run streamlit run app/main.py
```

## Ожидаемый результат

- В браузере работает ввод вопроса.
- Видны top-k фрагменты с `doc_id` и `score`.
- Есть минимум 3 demo-вопроса и 1 negative с отказом.

Дальше: [iter_08_tests_readme.md](iter_08_tests_readme.md)
