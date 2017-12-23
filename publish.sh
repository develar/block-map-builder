#!/usr/bin/env bash
set -e

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -z "$GITHUB_TOKEN" ] ; then
  SEC=`security find-generic-password -l GH_TOKEN -g 2>&1`
  export GITHUB_TOKEN=`echo "$SEC" | grep "password" | cut -d \" -f 2`
fi

NAME=block-map-builder
VERSION=0.0.1

checksums=""
publish()
{
  outDir=$1
  archiveName="$NAME-v$VERSION-$2"
  archiveFile="$BASEDIR/dist/$archiveName=.7z"
  rm -f "$archiveFile"

  cd "$BASEDIR/dist/$outDir"
  7za a -mx=9 -mfb=64 "$archiveFile" .

  CHECKSUM=$(shasum -a 512 "$archiveFile" | xxd -r -p | base64)
  github-release develar/block-map-builder "v$VERSION" master "" "$archiveFile"

  checksums="$checksums\n$archiveName $CHECKSUM"
}

publish "darwinamd64" mac
publish "linux386" linux-ia32
publish "linuxamd64" linux-x64
publish "windows386" win-ia32
publish "windowsamd64" win-x64

printf "$checksums\n"