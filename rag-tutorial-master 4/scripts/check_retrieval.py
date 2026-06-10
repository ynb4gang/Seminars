"""Проверка retrieval (итерация 5) — понятный вывод."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from app.retriever import Retriever


def print_hit(i: int, hit: dict) -> None:
    preview = hit["text"][:120].replace("\n", " ")
    print(f"  [{i}] doc_id={hit['doc_id']}, score={hit['score']:.4f}")
    print(f"      {preview}...")


def main() -> None:
    print("=== Проверка Retriever (итерация 5) ===\n")

    r = Retriever()
    print("OK: индекс загружен (vectorizer.pkl + matrix.npz + chunks.jsonl)\n")

    queries = [
        ("ипотека Citibank закрытие сделки", "должны найтись чанки по ипотеке и Citibank"),
        ("международный перевод Xoom задержка", "должны найтись чанки по денежному переводу Xoom"),
        ("как приготовить борщ", "negative-query: score должен быть ниже порога релевантности"),
    ]

    for query, hint in queries:
        print(f"Запрос: «{query}»")
        print(f"Ожидание: {hint}")
        results = r.search(query, k=3)
        print(f"Получено результатов: {len(results)}")
        for i, hit in enumerate(results, 1):
            print_hit(i, hit)
        print()

    print("=== Итог ===")
    print("Если видите результаты с полями doc_id / score / text, а negative-query имеет низкий score — retrieval работает.")


if __name__ == "__main__":
    main()
