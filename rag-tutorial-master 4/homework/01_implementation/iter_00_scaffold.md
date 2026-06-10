# Iter 00: Scaffold

Ориентир: [doc/tasklist.md](../../doc/tasklist.md)

## Что сделать

- Подготовить каркас проекта и окружение.

## Какие файлы

- `pyproject.toml`
- `.gitignore`
- `app/config.py`
- папки `app/`, `scripts/`, `data/raw/`, `data/processed/`, `data/index/`, `tests/`

## Как проверить

```bash
uv venv && uv sync
uv run python -c "import app.config"
```

## Ожидаемый результат

- Зависимости установлены без ошибок.
- Импорт `app.config` работает.

Дальше: [iter_01_demo_data.md](iter_01_demo_data.md)
