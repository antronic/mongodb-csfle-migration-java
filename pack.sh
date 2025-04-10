#!/bin/bash

cd ./mongodb-csfle-migrator
# Build the project
mvn clean package -DskipTests
# Check if the build was successful
if [ $? -ne 0 ]; then
  echo "Build failed. Exiting."
  exit 1
fi
# Check if the target directory exists
if [ ! -d "target/MongoDBCSFLEMigrator" ]; then
  echo "Target directory does not exist. Exiting."
  exit 1
fi

cd ..

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

# and rename them to remove the .sample extension
for file in ./*.sample.mongodb.js; do
  cp "$file" "$RELEASES_FILES_FOLDER/$(basename "${file/.sample/}")"
done

# Zip the releases directory
cd $RELEASES_FILES_FOLDER
zip -r MongoDBCSFLEMigrator.zip *
# 7z a -tzip -mx=9 -p<your-password> -mem=AES256 MongoDBCSFLEMigrator.zip *

# Move the zip file to the parent directory
mv MongoDBCSFLEMigrator.zip ../