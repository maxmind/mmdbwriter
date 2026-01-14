#!/bin/bash

set -eu -o pipefail

cd "$(git rev-parse --show-toplevel)"

check_command() {
    if ! command -v "$1" &>/dev/null; then
        echo "Error: $1 is not installed or not in PATH"
        exit 1
    fi
}

check_command gh

changelog=$(cat CHANGELOG.md)

version_regex='[0-9]+\.[0-9]+\.[0-9]+(-[^ ]+)?'
regex="
## ($version_regex) \(([0-9]{4}-[0-9]{2}-[0-9]{2})\)

((.|
)*)
"

if [[ ! $changelog =~ $regex ]]; then
      echo "Could not find date line in change log!"
      exit 1
fi

version="${BASH_REMATCH[1]}"
date="${BASH_REMATCH[3]}"
notes="$(echo "${BASH_REMATCH[4]}" | sed -n -E "/^## $version_regex/,\$!p")"
tag="v$version"

if [[ "$date" != "$(date +"%Y-%m-%d")" ]]; then
    echo "$date is not today!"
    exit 1
fi

if [ -n "$(git status --porcelain)" ]; then
    echo ". is not clean." >&2
    exit 1
fi

echo $'\nVersion:'
echo "$version"

echo $'\nRelease notes:'
echo "$notes"

read -e -p "Push to origin? [y/N] " should_push

if ! [[ "$should_push" =~ ^[Yy]$ ]]; then
    echo "Aborting"
    exit 1
fi

git push

gh release create --target "$(git branch --show-current)" -t "$version" -n "$notes" "$tag"
