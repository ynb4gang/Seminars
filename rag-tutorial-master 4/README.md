# Учебный RAG на открытых финансовых жалобах CFPB

Проект повторяет pipeline репозитория `MaratNotes/rag-tutorial` на собственном корпусе данных: **ingest → chunking → index → retrieval → demo-answer → Streamlit UI**.

RAG работает локально без внешней LLM: поиск выполняется через **TF-IDF + cosine similarity**, а demo-ответ формируется только из найденных фрагментов с указанием источников (`doc_id`, `score`, текст чанка).

## Данные

Источник: открытая база **Consumer Financial Protection Bureau Consumer Complaint Database**.

В проекте подготовлен учебный русскоязычный корпус `data/raw/datasets.json` из **1000 текстовых записей** по финансовым жалобам: ипотека, студенческие кредиты, кредитные карты, расчетные счета, денежные переводы, взыскание долга, автокредиты и ошибки кредитной истории.

Что индексируется: поле `text` из `datasets.json`. Метаданные `doc_id` и `name` используются только для вывода источников в UI.

Подробно: [`doc/DATA.md`](doc/DATA.md).

## Требования

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)

## Запуск

```bash
uv sync
uv run python scripts/build_index.py
uv run streamlit run app/main.py
```

После запуска Streamlit откроется по адресу: http://localhost:8501

## Проверка pipeline из консоли

```bash
# Сборка индекса
uv run python scripts/build_index.py

# Проверка retrieval
uv run python scripts/check_retrieval.py

# Проверка demo-answer и отказа
uv run python scripts/check_generator.py

# Тесты
uv run pytest tests/ -v
```

## Demo-вопросы

| Тип | Вопрос | Ожидаемое поведение |
|---|---|---|
| Demo 1 | `Какие проблемы бывают с ипотекой у Citibank?` | Ответ по фрагментам про ипотеку, источники с `doc_id` и `score` |
| Demo 2 | `Что известно про задержку международного перевода Xoom?` | Ответ по фрагментам про денежные переводы/Xoom |
| Demo 3 | `Какие жалобы связаны с ошибкой в кредитном отчете Experian?` | Ответ по фрагментам про кредитную историю |
| Negative | `Как приготовить борщ?` | Корректный отказ без выдумывания фактов |

## Пример логов

После сборки индекса:

```text
Записано 1000 документов -> data/processed/documents.jsonl
Записано 1000 чанков -> data/processed/chunks.jsonl
Документов: 1000, чанков: 3000, матрица: (3000, ...)
Индекс сохранён -> data/index
```

Проверка negative-case:

```text
--- Negative: «Как приготовить борщ?» ---
Ответ:
В базе не найдено релевантных фрагментов. Ответить по данным невозможно.
```

## Структура проекта

```text
app/
  chunker.py       # нарезка документов на чанки
  config.py        # пути, top_k, размер чанка, overlap
  generator.py     # demo-ответ и отказ
  main.py          # Streamlit UI
  prompts.py       # правила ответа и порог релевантности
  retriever.py     # TF-IDF retrieval
scripts/
  ingest.py        # datasets.json -> documents.jsonl
  build_index.py   # ingest -> chunking -> TF-IDF index
  check_retrieval.py
  check_generator.py
data/raw/
  datasets.json    # 1000 исходных текстовых записей
doc/
  DATA.md          # описание данных
  *.md             # документы планирования
tests/
  test_chunking.py
  test_retrieval.py
homework/
  SUBMISSION.md
  IMPROVEMENTS.md
```

## Реализованное улучшение

В MVP добавлен диагностический Streamlit UI: пользователь видит не только итоговый ответ, но и найденные top-k фрагменты, `doc_id`, `score` и полный текст источника. Это помогает проверять, почему RAG ответил или отказался отвечать.

## Ограничения

- TF-IDF ищет по словам, а не по смысловым embeddings.
- Demo-answer не вызывает внешнюю LLM, поэтому ответ является консервативной сборкой найденного контекста.
- Для production-версии лучше добавить embeddings, reranking и нормальную генерацию через LLM с обязательными citations.
