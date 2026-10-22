#!/usr/bin/env bash
# Build docs/onboarding/manifester-onboarding-book.epub from the onboarding chapters.
# Toolchain: pandoc (Markdown -> EPUB 3). Requires: pandoc, python3.
# EPUB is reflowable, so Send to Kindle and Kindle apps can resize text.
set -euo pipefail

ONBOARDING_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT="$ONBOARDING_DIR/manifester-onboarding-book.epub"

for tool in pandoc python3; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "error: '$tool' not found. Install with: brew install pandoc" >&2
    exit 1
  }
done

BUILD_DIR="$(mktemp -d "${TMPDIR:-/tmp}/manifester-book-epub.XXXXXX")"
trap 'rm -rf "$BUILD_DIR"' EXIT

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
    label = "**Answer:** " if summary == "Answer" else f"**{summary}** "
    return f"{label}{body}"

def rewrite_link(m):
    text, url = m.group(1), m.group(2).strip()
    if url.startswith(("http://", "https://", "mailto:", "#")):
        return m.group(0)
    path, _, frag = url.partition("#")
    base = path.rsplit("/", 1)[-1]
    if base in chapter_anchors:
        return f"[{text}](#{frag if frag else chapter_anchors[base]})"
    return text

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
    if lines and lines[0].startswith("# "):
        lines[0] = ("# Introduction {#introduction}\n" if name == "README.md"
                    else lines[0].rstrip() + f" {{#{chapter_anchors[name]}}}\n")
    text = "".join(lines)
    parts.append("".join(
        seg if seg.startswith(("```", "~~~")) else process_plain(seg)
        for seg in fence_re.split(text)))

out_path.write_text("\n\n".join(p.rstrip() for p in parts) + "\n", encoding="utf-8")
print(f"combined {len(files)} files -> {out_path}")
PY

cat > "$BUILD_DIR/epub.css" <<'CSS'
body { line-height: 1.55; }
h1 { page-break-before: always; }
code { font-family: monospace; font-size: 0.82em; overflow-wrap: anywhere; }
pre { font-family: monospace; font-size: 0.72em; line-height: 1.35; white-space: pre-wrap; overflow-wrap: anywhere; border: 1px solid #d0d7de; padding: 0.6em; }
pre code { font-size: 1em; }
table { border-collapse: collapse; width: 100%; font-size: 0.78em; margin: 1em 0; }
th, td { border: 1px solid #d0d7de; padding: 0.35em 0.5em; vertical-align: top; overflow-wrap: break-word; }
th { text-align: left; }
blockquote { border-left: 3px solid #d0d7de; margin-left: 0; padding-left: 1em; color: #444; }
a { overflow-wrap: anywhere; }
CSS

pandoc "$BUILD_DIR/combined.md" \
  --toc --toc-depth=2 \
  --split-level=1 \
  --css="$BUILD_DIR/epub.css" \
  --metadata title="Manifester Onboarding Book" \
  --metadata author="Arun Lakshman" \
  --metadata lang="en" \
  --metadata date="2026-10-21" \
  --metadata description="What Manifester is, how it works, and how to make a first change safely. Kafka on S3, a deterministic core, a DynamoDB log control plane, and the simulator that replays it." \
  --output "$OUT"

echo "Wrote $OUT"
ls -lh "$OUT"
