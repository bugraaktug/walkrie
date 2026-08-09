#!/usr/bin/env bash
# Produces packaging/rpm/walkrie-<version>.tar.gz for `rpmbuild -ba walkrie.spec`.
#
# rpmbuild environments (mock/koji especially) commonly run offline, so
# the tarball vendors third_party/llama.cpp's submodule content directly
# rather than relying on `git submodule update` at build time — `git
# archive` alone doesn't recurse into submodules, so that content is
# copied in separately below.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
git submodule update --init --recursive

VERSION=$(awk '/^Version:/{print $2; exit}' packaging/rpm/walkrie.spec)
NAME="walkrie-${VERSION}"
OUT_DIR="packaging/rpm"

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

git archive --format=tar --prefix="${NAME}/" HEAD | tar -x -C "$TMPDIR"

mkdir -p "$TMPDIR/${NAME}/third_party/llama.cpp"
tar -C third_party/llama.cpp --exclude=.git -cf - . \
    | tar -x -C "$TMPDIR/${NAME}/third_party/llama.cpp"

tar -czf "${OUT_DIR}/${NAME}.tar.gz" -C "$TMPDIR" "${NAME}"
echo "wrote ${OUT_DIR}/${NAME}.tar.gz"
