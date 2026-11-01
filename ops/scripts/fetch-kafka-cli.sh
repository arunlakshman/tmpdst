#!/usr/bin/env bash
# Download Apache Kafka 3.9.1 into .tools/kafka-3.9.1/ (git-ignored).
# Checks the sha512 from the Apache site and the sha512 pinned below.
# Skips the download when the dir already has the tools. Prints the bin dir
# as the last line.
set -euo pipefail
cd "$(dirname "$0")/../.."

KAFKA_VERSION=3.9.1
KAFKA_TGZ=kafka_2.13-${KAFKA_VERSION}.tgz
KAFKA_URL=https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}
# sha512 of KAFKA_TGZ, lowercase hex. Taken from the Apache .sha512 file.
KAFKA_SHA512=1ea204ba73411737a275429ca976d440f007ff0957b90b19be41dc5a4bae52617267769be9f0b5791714d0b3c4c760605bd426faea39edd90763585523fa2cfe
DEST=.tools/kafka-${KAFKA_VERSION}
BIN_DIR=$PWD/$DEST/bin

# Print the hash in an Apache .sha512 file. The file looks like
# "name.tgz: HASH" and the hash may be split in groups over several lines.
# Drop the name, spaces and newlines, then lowercase.
read_sha512_file() {
  sed -e 's/^[^:]*://' "$1" | tr -d ' \t\r\n' | tr 'A-F' 'a-f'
}

if [[ -x $BIN_DIR/kafka-console-producer.sh ]]; then
  echo "Kafka ${KAFKA_VERSION} already there."
  echo "$BIN_DIR"
  exit 0
fi

if [[ ! $KAFKA_SHA512 =~ ^[0-9a-f]{128}$ ]]; then
  echo "KAFKA_SHA512 is not set to a sha512 hex value in $0" >&2
  exit 1
fi

# The dir must be new or empty, so the extract starts clean.
if [[ -e $DEST && -n "$(ls -A "$DEST")" ]]; then
  echo "$DEST exists and is not empty but has no bin/. Remove it and run again." >&2
  exit 1
fi

# The download is kept in .tools/cache so a stop or a failed check does not
# lose it. curl -C - continues a half file.
tmp=$PWD/.tools/cache
mkdir -p "$DEST" "$tmp"

echo "Downloading $KAFKA_URL"
curl -fSL --retry 5 -C - -o "$tmp/$KAFKA_TGZ" "$KAFKA_URL"
curl -fsSL --retry 3 -o "$tmp/$KAFKA_TGZ.sha512" "$KAFKA_URL.sha512"

actual=$(shasum -a 512 "$tmp/$KAFKA_TGZ" | awk '{print $1}')
published=$(read_sha512_file "$tmp/$KAFKA_TGZ.sha512")
if [[ $actual != "$published" ]]; then
  echo "sha512 does not match the Apache .sha512 file" >&2
  echo "  file:   $published" >&2
  echo "  actual: $actual" >&2
  exit 1
fi
if [[ $actual != "$KAFKA_SHA512" ]]; then
  echo "sha512 does not match the pinned KAFKA_SHA512 in $0" >&2
  echo "  pinned: $KAFKA_SHA512" >&2
  echo "  actual: $actual" >&2
  exit 1
fi
echo "sha512 OK"

tar -xzf "$tmp/$KAFKA_TGZ" -C "$DEST" --strip-components=1
echo "Kafka ${KAFKA_VERSION} ready."
echo "$BIN_DIR"
