#!/usr/bin/env bash
. wvtest.sh
. wvtest-kurt.sh

#set -e

TOP="$(/bin/pwd)"

kurt()
{
    "$TOP/kurt" "$@"
}

WVSTART "pack"

tmpdir="$(wvmktempdir)"
WVPASS kurt -o $tmpdir $TOP
rm -rf "$tmpdir"

