# Iter 06: Demo Answer

Ориентир: [app/prompts.py](../../app/prompts.py), [app/generator.py](../../app/generator.py), [scripts/check_generator.py](../../scripts/check_generator.py)

## Что сделать

- Реализовать ответ только по найденным чанкам.
- Добавить отказ, если релевантного контекста нет.

## Какие файлы

- `app/prompts.py`
- `app/generator.py`

## Как проверить

```bash
uv run python scripts/check_generator.py
```

## Ожидаемый результат

- Релевантный запрос дает ответ + источники.
- Нерелевантный запрос дает явный отказ без выдумок.

Дальше: [iter_07_streamlit_ui.md](iter_07_streamlit_ui.md)
