#!/bin/bash
RELEASES_FOLDER="./releases"
RELEASES_FOLDER=$(realpath "$RELEASES_FOLDER")
RELEASES_FILES_FOLDER="$RELEASES_FOLDER/files"

rm -rf ./releases/*
mkdir -p $RELEASES_FILES_FOLDER
cp ./mongodb-csfle-migrator/target/MongoDBCSFLEMigrator/*.jar $RELEASES_FILES_FOLDER

# Copy the sample JSON files to the releases directory
# and rename them to remove the .sample extension
for file in ./mongodb-csfle-migrator/*.sample.json; do
  cp "$file" "$RELEASES_FILES_FOLDER/$(basename "${file/.sample/}")"
done

# Zip the releases directory
cd $RELEASES_FILES_FOLDER
zip -r MongoDBCSFLEMigrator.zip *

# Move the zip file to the parent directory
mv MongoDBCSFLEMigrator.zip ../