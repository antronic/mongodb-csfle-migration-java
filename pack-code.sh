#!/bin/bash

# cd ./mongodb-csfle-migrator

PASSWORD_FILE_PATH="password.hidden.txt"

PACK_CODE_FOLDER="./tmp/packed"
mkdir -p $PACK_CODE_FOLDER
PACK_CODE_FOLDER=$(realpath "$PACK_CODE_FOLDER")

PROJECT_NAME="mongodb-csfle-migrator"

SOURCE_SOURCE_CODE_FOLDER="./$PROJECT_NAME"
SOURCE_DOC_FOLDER="./tmp/docs"
SOURCE_SAMPLE_DATA_FOLDER="./tmp/sample-data"
SOURCE_MONGOSH_FOLDER="./mongosh"

TARGET_SOURCE_CODE_FOLDER="$PACK_CODE_FOLDER/.tmp/code/$PROJECT_NAME"
TARGET_DOC_FOLDER="$PACK_CODE_FOLDER/.tmp/docs"
TARGET_SAMPLE_DATA_FOLDER="$PACK_CODE_FOLDER/.tmp/sample-data"
TARGET_MONGOSH_FOLDER="$PACK_CODE_FOLDER/.tmp/mongosh"

TARGET_OUTPUT_FOLDER="$PACK_CODE_FOLDER/OUTPUT"

# Check if file exists
if [[ ! -f "$PASSWORD_FILE_PATH" ]]; then
  echo "Error: File '$PASSWORD_FILE_PATH' not found."
  exit 1
fi

# Read value from file (first line, as example)
PASSWORD=$(<"$PASSWORD_FILE_PATH")

# Clean up the target folders, including hidden files
rm -rf $PACK_CODE_FOLDER/*
rm -rf $PACK_CODE_FOLDER/.*

mkdir -p $TARGET_SOURCE_CODE_FOLDER
mkdir -p $TARGET_DOC_FOLDER
mkdir -p $TARGET_SAMPLE_DATA_FOLDER
mkdir -p $TARGET_MONGOSH_FOLDER
mkdir -p $TARGET_OUTPUT_FOLDER/.tmp

TARGET_OUTPUT_FOLDER=$(realpath "$TARGET_OUTPUT_FOLDER")
# Copy the source code to the target folder
cp -r $SOURCE_SOURCE_CODE_FOLDER/src $TARGET_SOURCE_CODE_FOLDER
cp -r $SOURCE_SOURCE_CODE_FOLDER/pom.xml $TARGET_SOURCE_CODE_FOLDER
# cp -r $SOURCE_SOURCE_CODE_FOLDER/README.md $TARGET_SOURCE_CODE_FOLDER

# Copy sample config

# Copy the sample JSON files to the releases directory
# and rename them to remove the .sample extension
for file in $SOURCE_SOURCE_CODE_FOLDER/*.sample.json; do
  cp "$file" "$TARGET_SOURCE_CODE_FOLDER/$(basename "${file/.sample/}")"
done

# and rename them to remove the .sample extension
for file in ./*.sample.mongodb.js; do
  cp "$file" "$TARGET_SOURCE_CODE_FOLDER/$(basename "${file/.sample/}")"
done

# Copy the sample data to the target folder
cp -r $SOURCE_SAMPLE_DATA_FOLDER/* $TARGET_SAMPLE_DATA_FOLDER
# Copy the docs to the target folder
cp -r $SOURCE_DOC_FOLDER/* $TARGET_DOC_FOLDER
# Copy the mongosh scripts to the target folder
cp -r $SOURCE_MONGOSH_FOLDER/* $TARGET_MONGOSH_FOLDER

echo "Packing code..."
echo "Packing code to $TARGET_OUTPUT_FOLDER"
echo "Packing code to $TARGET_OUTPUT_FOLDER/.tmp"
TARGET_OUTPUT_TMP_FOLDER="$TARGET_OUTPUT_FOLDER/.tmp"

# Compress into 7z archives
7z a -t7z -mx=9 -m0=lzma2 -mhe=on $TARGET_OUTPUT_TMP_FOLDER/sample-test-data.7z $TARGET_SAMPLE_DATA_FOLDER
7z a -t7z -mx=9 -m0=lzma2 -mhe=on $TARGET_OUTPUT_TMP_FOLDER/mongosh_scripts.7z $TARGET_MONGOSH_FOLDER
7z a -t7z -mx=9 -m0=lzma2 -mhe=on $TARGET_OUTPUT_TMP_FOLDER/source_code.7z $TARGET_SOURCE_CODE_FOLDER

# Compress all 7z into a zip format
# zip .docx
7z a -tzip -mx=9 -p"$PASSWORD" -mem=AES256 $TARGET_OUTPUT_TMP_FOLDER/docs.zip $TARGET_DOC_FOLDER
7z a -tzip -mx=9 -p"$PASSWORD" -mem=AES256 $TARGET_OUTPUT_TMP_FOLDER/sample-test-data.zip $TARGET_OUTPUT_TMP_FOLDER/sample-test-data.7z
7z a -tzip -mx=9 -p"$PASSWORD" -mem=AES256 $TARGET_OUTPUT_TMP_FOLDER/mongosh_scripts.zip $TARGET_OUTPUT_TMP_FOLDER/mongosh_scripts.7z
7z a -tzip -mx=9 -p"$PASSWORD" -mem=AES256 $TARGET_OUTPUT_TMP_FOLDER/source_code.zip $TARGET_OUTPUT_TMP_FOLDER/source_code.7z
# 7z a -tzip -mx=9 -p"$PASSWORD" -mem=AES256 $TARGET_OUTPUT_TMP_FOLDER/docs.zip $TARGET_DOC_FOLDER

DATE_TIME=$(date +%Y-%m-%d_%H-%M-%S)
FILE_NAME="sample-mongodb-csfle-migrator_app_$DATE_TIME"

7z a -tzip -mx=9 -mem=AES256 $TARGET_OUTPUT_FOLDER/$FILE_NAME \
$TARGET_OUTPUT_TMP_FOLDER/sample-test-data.zip \
$TARGET_OUTPUT_TMP_FOLDER/source_code.zip \
$TARGET_OUTPUT_TMP_FOLDER/docs.zip \
$TARGET_OUTPUT_TMP_FOLDER/mongosh_scripts.zip

echo "Packing with Password: $PASSWORD"