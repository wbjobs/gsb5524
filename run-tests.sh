#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

BUILD_DIR="build"
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR/classes"

mapfile -t SOURCES < <(find src -name '*.java' | sort)
if [ "${#SOURCES[@]}" -eq 0 ]; then
  echo "no sources found under src/" >&2
  exit 1
fi

JAVAC_VERSION="$(javac -version 2>&1 | awk '{print $2}')"
MAJOR="${JAVAC_VERSION%%.*}"
if [ "$MAJOR" = "1" ]; then
  MAJOR="$(echo "$JAVAC_VERSION" | cut -d. -f2)"
fi

if [ "$MAJOR" -ge 9 ]; then
  javac --release 8 -encoding UTF-8 -d "$BUILD_DIR/classes" "${SOURCES[@]}"
else
  javac -source 8 -target 8 -encoding UTF-8 -d "$BUILD_DIR/classes" "${SOURCES[@]}"
fi

java -cp "$BUILD_DIR/classes" com.gsb.quota.tests.TestRunner
