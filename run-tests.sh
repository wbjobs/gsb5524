#!/usr/bin/env bash
#
# Builds and runs the multi-tenant quota scheduler tests.
# Requires only a JDK (8 or newer): javac + java. No Maven/Gradle/JUnit.
set -euo pipefail

cd "$(dirname "$0")"

BUILD=build
rm -rf "$BUILD"
mkdir -p "$BUILD/main" "$BUILD/test"

echo "== compiling main sources =="
find src -name '*.java' | sort > "$BUILD/main-sources.txt"
javac -source 8 -target 8 -encoding UTF-8 -Xlint:all -d "$BUILD/main" @"$BUILD/main-sources.txt"

echo "== compiling test sources =="
find test -name '*.java' | sort > "$BUILD/test-sources.txt"
javac -source 8 -target 8 -encoding UTF-8 -cp "$BUILD/main" -d "$BUILD/test" @"$BUILD/test-sources.txt"

echo "== running tests =="
java -cp "$BUILD/main:$BUILD/test" com.gsb.quota.TestMain
