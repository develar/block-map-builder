#!/usr/bin/env bash
set -e

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -z "$GITHUB_TOKEN" ] ; then
  SEC=`security find-generic-password -l GH_TOKEN -g 2>&1`
  export GITHUB_TOKEN=`echo "$SEC" | grep "password" | cut -d \" -f 2`
fi

NAME=block-map-builder
VERSION=0.1.0

OUT_DIR="$BASEDIR/dist/out"
rm -rf "$OUT_DIR"

publish()
{
  outDir=$1
  archiveName="$NAME-v$VERSION-$2"
  archiveFile="$OUT_DIR/$archiveName.7z"

  cd "$BASEDIR/dist/$outDir"
  7za a -mx=9 -mfb=64 "$archiveFile" .
}

publish "darwinamd64" mac
publish "linux386" linux-ia32
publish "linuxamd64" linux-x64
publish "windows386" win-ia32
publish "windowsamd64" win-x64

tool-releaser develar/block-map-builder "v$VERSION" master "" "$OUT_DIR/*.7z"