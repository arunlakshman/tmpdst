#!/usr/bin/env bash
# Build docs/onboarding/manifester-onboarding-book.pdf from the onboarding chapters.
# Toolchain: pandoc (Markdown -> standalone HTML + TOC) + WeasyPrint (HTML -> PDF).
# Requires: pandoc, weasyprint, python3  (e.g. `brew install pandoc weasyprint`)
set -euo pipefail

ONBOARDING_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT="$ONBOARDING_DIR/manifester-onboarding-book.pdf"

for tool in pandoc weasyprint python3; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "error: '$tool' not found. Install with: brew install pandoc weasyprint" >&2
    exit 1
  }
done

BUILD_DIR="$(mktemp -d "${TMPDIR:-/tmp}/manifester-book.XXXXXX")"
trap 'rm -rf "$BUILD_DIR"' EXIT

# --- 1. Concatenate + preprocess chapters (does not modify source .md files) ---
python3 - "$ONBOARDING_DIR" "$BUILD_DIR/combined.md" <<'PY'
import re, sys
from pathlib import Path

src_dir, out_path = Path(sys.argv[1]), Path(sys.argv[2])
files = [
    "README.md",
    "01-the-big-picture.md", "02-map-of-the-repository.md",
    "03-the-deterministic-core.md", "04-broker-data-plane.md",
    "05-wire-and-storage-formats.md", "06-control-plane.md",
    "07-production-runtime.md", "08-deterministic-simulation.md",
    "09-verification-and-quality-gates.md",
    "10-deployment-benchmarking-observability.md",
    "11-decisions-history-and-roadmap.md", "12-newcomer-playbook.md",
]

def anchor_for(name):
    if name == "README.md":
        return "introduction"
    return f"chapter-{int(name.split('-', 1)[0])}"

chapter_anchors = {name: anchor_for(name) for name in files}

def clean_summary(text):
    text = re.sub(r"<code>(.*?)</code>", r"`\1`", text, flags=re.S)
    text = re.sub(r"<[^>]+>", "", text)
    return re.sub(r"\s+", " ", text).strip().replace("**", "")

def details_repl(m):
    summary, body = clean_summary(m.group(1)), m.group(2).strip()
    label = "**Answer:** " if summary == "Answer" else f"**{summary}** — "
    return f"{label}{body}"

def rewrite_link(m):
    text, url = m.group(1), m.group(2).strip()
    if url.startswith(("http://", "https://", "mailto:", "#")):
        return m.group(0)
    path, _, frag = url.partition("#")
    base = path.rsplit("/", 1)[-1]
    if base in chapter_anchors:  # inter-chapter link -> internal anchor
        return f"[{text}](#{frag if frag else chapter_anchors[base]})"
    return text  # link to a file outside the book -> keep text, drop file:// target

fence_re = re.compile(r"(```.*?```|~~~.*?~~~)", re.S)
link_re = re.compile(r"\[([^\]]*)\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
details_re = re.compile(r"<details>\s*<summary>(.*?)</summary>(.*?)</details>", re.S)

def process_plain(segment):
    segment = details_re.sub(details_repl, segment)
    segment = re.sub(r"</?details>", "", segment)
    segment = re.sub(r"<summary>(.*?)</summary>",
                     lambda m: f"**{clean_summary(m.group(1))}**", segment, flags=re.S)
    return link_re.sub(rewrite_link, segment)

parts = []
for name in files:
    lines = (src_dir / name).read_text(encoding="utf-8").splitlines(keepends=True)
    if lines and lines[0].startswith("# "):  # title page already names the book
        lines[0] = ("# Introduction {#introduction}\n" if name == "README.md"
                    else lines[0].rstrip() + f" {{#{chapter_anchors[name]}}}\n")
    text = "".join(lines)
    parts.append("".join(
        seg if seg.startswith(("```", "~~~")) else process_plain(seg)
        for seg in fence_re.split(text)))

out_path.write_text("\n\n".join(p.rstrip() for p in parts) + "\n", encoding="utf-8")
print(f"combined {len(files)} files -> {out_path}")
PY

# --- 2. Print stylesheet ---
cat > "$BUILD_DIR/style.css" <<'CSS'
@page {
  size: A4; margin: 22mm 17mm 20mm 17mm;
  @bottom-center { content: "Manifester Onboarding Book · page " counter(page) " of " counter(pages);
                   font-family: "Helvetica Neue", Helvetica, Arial, sans-serif; font-size: 8pt; color: #666; }
  @top-left { content: "Manifester Onboarding Book";
              font-family: "Helvetica Neue", Helvetica, Arial, sans-serif; font-size: 8pt; color: #888; }
}
@page :first { @top-left { content: none; } @bottom-center { content: none; } }
html { font-size: 10.5pt; }
body { font-family: "Helvetica Neue", Helvetica, Arial, sans-serif; color: #1f2328; line-height: 1.5; margin: 0; }
header#title-block-header { text-align: center; padding-top: 55mm; break-after: page; }
header#title-block-header h1.title { font-size: 32pt; line-height: 1.2; border: none; margin: 0 0 8mm 0; }
header#title-block-header p.subtitle { font-size: 14pt; color: #444; }
header#title-block-header p.date, header#title-block-header p.author { font-size: 11pt; color: #666; }
nav#TOC { break-after: page; font-size: 9.5pt; line-height: 1.45; }
nav#TOC > ul { list-style: none; padding-left: 0; }
nav#TOC ul ul { list-style: none; padding-left: 5mm; }
nav#TOC a { color: #1f2328; text-decoration: none; }
h1 { font-size: 20pt; line-height: 1.25; break-before: page; break-after: avoid; border-bottom: 2px solid #d0d7de; padding-bottom: 2mm; }
h2 { font-size: 14pt; break-after: avoid; margin-top: 7mm; }
h3 { font-size: 12pt; break-after: avoid; }
h4, h5, h6 { break-after: avoid; }
a { color: #0969da; text-decoration: none; overflow-wrap: anywhere; }
code { font-family: "SF Mono", Menlo, Consolas, "DejaVu Sans Mono", monospace; font-size: 8.5pt;
       background: #f6f8fa; padding: 0.1em 0.25em; border-radius: 3px; overflow-wrap: anywhere; }
pre { background: #f6f8fa; border: 1px solid #d8dee4; border-radius: 4px; padding: 3mm; break-inside: auto; }
pre code { font-size: 7.5pt; line-height: 1.35; padding: 0; background: none;
           white-space: pre-wrap; word-wrap: break-word; overflow-wrap: anywhere; }
table { border-collapse: collapse; width: 100%; font-size: 8pt; margin: 4mm 0; }
thead { display: table-header-group; }
tr { break-inside: avoid; }
th, td { border: 1px solid #d0d7de; padding: 1.2mm 1.8mm; vertical-align: top; overflow-wrap: break-word; }
th { background: #f6f8fa; text-align: left; }
td code, th code { font-size: 7.5pt; }
blockquote { border-left: 3px solid #d0d7de; color: #444; margin-left: 0; padding-left: 4mm; }
hr { border: none; border-top: 1px solid #d0d7de; }
img { max-width: 100%; }
CSS

# --- 3. Markdown -> HTML (with TOC) -> PDF ---
pandoc "$BUILD_DIR/combined.md" \
  --standalone --toc --toc-depth=2 \
  --metadata title="Manifester Onboarding Book" \
  --metadata subtitle="What Manifester is, how it works, and how to make your first change safely" \
  --metadata date="Generated from docs/onboarding (README + Chapters 1–12)" \
  --css=style.css \
  --output "$BUILD_DIR/book.html"

weasyprint "$BUILD_DIR/book.html" "$OUT"

echo "Wrote $OUT"
command -v pdfinfo >/dev/null 2>&1 && pdfinfo "$OUT" | grep -E '^(Pages|File size|Page size):' || ls -lh "$OUT"
