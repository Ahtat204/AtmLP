#!/bin/bash
set -o pipefail
rootDir=$(git rev-parse --show-toplevel)
install_pkg() {
    local pkg="$1"
    if command -v apt-get &> /dev/null; then
        sudo apt-get update && sudo apt-get install -y "$pkg"
    elif command -v dnf &> /dev/null; then
        sudo dnf install -y "$pkg"
    elif command -v yum &> /dev/null; then
        sudo yum install -y "$pkg"
    elif command -v pacman &> /dev/null; then
        sudo pacman -S --noconfirm "$pkg"
    elif command -v zypper &> /dev/null; then
        sudo zypper install -y "$pkg"
    elif command -v apk &> /dev/null; then
        sudo apk add --no-cache "$pkg"
    else
        echo "Error: Unsupported package manager." >&2
        return 1
    fi
}

if ! command -v curl >/dev/null 2>&1;then
    install_pkg curl;
fi
curl -L -o "$rootDir/04_Datasets/Historical_Data/data.zip" "https://www.kaggle.com/api/v1/datasets/download/obinnaiheanachor/wisabi-bank-dataset"
unzip "$rootDir/04_Datasets/Historical_Data/data.zip" -d "$rootDir/04_Datasets/Historical_Data/" && rm "$rootDir/04_Datasets/Historical_Data/data.zip"
cd "$rootDir/04_Datasets/Historical_Data/" || exit ;
for f in *\ *
 do mv -n "$f" "${f// /_}" 2>/dev/null
done;
