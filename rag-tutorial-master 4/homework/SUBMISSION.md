# Submission

## Ссылка на репозиторий с заданием

- Repo URL: `https://github.com/<your-username>/rag-homework`

## Автор

- ФИО / ник: Владимир Носов

## Что реализовано

- Рабочий RAG pipeline: `ingest -> chunking -> index -> retrieval -> demo-answer -> Streamlit UI`.
- Локальный запуск через `uv sync`, `uv run python scripts/build_index.py`, `uv run streamlit run app/main.py`.
- Корпус данных `data/raw/datasets.json` на 1000 текстовых записей по финансовым жалобам CFPB.
- UI показывает найденные источники: `doc_id`, `score`, название и текст чанка.
- Добавлены demo-вопросы и negative-вопрос с корректным отказом.
- Добавлены тесты для chunking, retrieval, источников и отказа.

## Улучшение

Реализован диагностический вывод источников в Streamlit: пользователь видит top-k фрагменты, score и текст, поэтому можно проверить, почему RAG дал ответ или отказался отвечать.
