"""Проверка demo-ответа (итерация 6)."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from app.generator import ask


def show(label: str, question: str) -> None:
    print(f"\n--- {label}: «{question}» ---")
    result = ask(question)
    print(f"Ответ:\n{result['answer']}\n")
    print(f"Источников: {len(result['sources'])}")
    for i, src in enumerate(result["sources"], 1):
        print(f"  [{i}] doc_id={src['doc_id']}, score={src['score']:.4f}, name={src['name'][:50]}...")


if __name__ == "__main__":
    show("Demo 1", "Какие проблемы бывают с ипотекой у Citibank?")
    show("Demo 2", "Что известно про задержку международного перевода Xoom?")
    show("Demo 3", "Какие жалобы связаны с ошибкой в кредитном отчете Experian?")
    show("Negative", "Как приготовить борщ?")
