#!/usr/bin/env bash
# Builds the walkrie RPM end to end: source tarball -> rpmbuild -> copy the
# resulting .rpm(s) into packaging/rpm/dist/ (gitignored) for easy pickup.
#
# Usage: ./packaging/rpm/build.sh
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

./packaging/rpm/make-tarball.sh

TOPDIR="$(mktemp -d)"
trap 'rm -rf "$TOPDIR"' EXIT
mkdir -p "$TOPDIR"/{SOURCES,SPECS,BUILD,RPMS,SRPMS,BUILDROOT}

VERSION=$(awk '/^Version:/{print $2; exit}' packaging/rpm/walkrie.spec)
cp "packaging/rpm/walkrie-${VERSION}.tar.gz" "$TOPDIR/SOURCES/"
cp packaging/rpm/walkrie.changelog "$TOPDIR/SOURCES/"
cp packaging/rpm/walkrie.spec "$TOPDIR/SPECS/"

rpmbuild --define "_topdir $TOPDIR" -ba "$TOPDIR/SPECS/walkrie.spec"

DIST_DIR="packaging/rpm/dist"
mkdir -p "$DIST_DIR"
find "$TOPDIR/RPMS" "$TOPDIR/SRPMS" -name '*.rpm' -exec cp {} "$DIST_DIR/" \;

echo "Built packages:"
ls -la "$DIST_DIR"
