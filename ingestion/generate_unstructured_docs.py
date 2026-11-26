"""Generate deterministic synthetic PDFs and PNGs for OCR evaluation."""

from __future__ import annotations

import argparse
import random
import textwrap
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFilter, ImageFont
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from common.config import get_settings
from common.lineage import emit_event
from common.telemetry import traced
from ingestion import atomic_write_json
from ingestion.fixtures import (
    DOC_TYPE_META,
    apply_char_confusion,
    choose_document_templates,
    document_template_hash,
    render_template,
)


def _wrapped_lines(text: str, width: int = 88) -> list[str]:
    lines: list[str] = []
    for paragraph in text.splitlines():
        lines.extend(textwrap.wrap(paragraph, width=width) or [""])
    return lines


def _write_pdf(path: Path, pages: list[str], rotation: float = 0.0) -> None:
    pdf = canvas.Canvas(str(path), pagesize=letter)
    for text in pages:
        pdf.saveState()
        if rotation:
            pdf.translate(306, 396)
            pdf.rotate(rotation)
            pdf.translate(-306, -396)
        y = 756
        pdf.setFont("Helvetica", 9)
        for line in _wrapped_lines(text):
            pdf.drawString(42, y, line)
            y -= 11
            if y < 36:
                break
        pdf.restoreState()
        pdf.showPage()
    pdf.save()


def _write_png(path: Path, text: str, rotation: float = 0.0, blur: bool = False) -> None:
    image = Image.new("RGB", (1700, 2200), "white")
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default(size=18)
    y = 70
    for line in _wrapped_lines(text, 100):
        draw.text((80, y), line, fill="black", font=font)
        y += 25
        if y > 2120:
            break
    if rotation:
        image = image.rotate(rotation, resample=Image.Resampling.BICUBIC, fillcolor="white")
    if blur:
        image = image.filter(ImageFilter.GaussianBlur(radius=0.7))
    image.save(path, format="PNG")


def generate_documents(batch_id: str, count: int, data_dir: Path) -> list[dict[str, Any]]:
    """Generate ``count`` files and return reproducible provenance entries."""
    rng = random.Random(42)
    output_dir = data_dir / "unstructured" / batch_id
    output_dir.mkdir(parents=True, exist_ok=True)
    entries: list[dict[str, Any]] = []
    doc_types = tuple(DOC_TYPE_META)
    for index in range(count):
        doc_type = doc_types[index % len(doc_types)]
        file_type = "pdf" if index % 10 < 6 else "png"
        pages = rng.randint(1, 2) if file_type == "pdf" else 1
        perturbed = index % 2 == 0
        templates = choose_document_templates(doc_type, rng, pages)
        rendered = [render_template(template, rng)[0] for template in templates]
        if perturbed:
            rendered = [apply_char_confusion(text, rng) for text in rendered]
        filename = f"{index + 1:05d}_{doc_type}.{file_type}"
        path = output_dir / filename
        rotation = rng.choice((-2.0, -1.0, 1.0, 2.0)) if perturbed else 0.0
        if file_type == "pdf":
            _write_pdf(path, rendered, rotation)
        else:
            _write_png(path, rendered[0], rotation, blur=perturbed)
        meta = DOC_TYPE_META[doc_type]
        entries.append(
            {
                "filename": filename,
                "relative_file_path": filename,
                "doc_type": doc_type,
                "domain": meta["domain"],
                "language": meta["language"],
                "template_hash": document_template_hash(templates),
                "perturbed": perturbed,
                "pages": pages,
                "file_type": file_type,
            }
        )
    atomic_write_json(
        output_dir / "manifest.json",
        {"batch_id": batch_id, "count": count, "seed": 42, "documents": entries},
    )
    print(f"generated {count} documents in {output_dir}")
    return entries


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--count", type=int, default=1000)
    args = parser.parse_args(argv)
    if args.count < 0:
        parser.error("--count must be non-negative")
    settings = get_settings()
    emit_event("ingestion.generate_unstructured_docs", "START", outputs=["data.unstructured"])
    with traced("ingestion.generate_unstructured_docs", attributes={"batch_id": args.batch_id}):
        entries = generate_documents(args.batch_id, args.count, settings.data_dir)
    emit_event(
        "ingestion.generate_unstructured_docs",
        "COMPLETE",
        outputs=["data.unstructured"],
        facets={"files": len(entries)},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
