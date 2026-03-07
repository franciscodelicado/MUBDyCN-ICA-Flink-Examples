#!/bin/bash
#
#  Script to install the Orion-Flink Connector into the local Maven repository

echo -e "\n⏳\033[1;34mInstalling Orion-Flink Connector into local Maven repository...\033[0m\n"

curl -LO https://github.com/ging/fiware-cosmos-orion-flink-connector/releases/download/1.2.4/orion.flink.connector-1.2.4.jar
mvn install:install-file \
  -Dfile=./orion.flink.connector-1.2.4.jar \
  -DgroupId=org.fiware.cosmos \
  -DartifactId=orion.flink.connector \
  -Dversion=1.2.4 \
  -Dpackaging=jar

echo -e "\n🎊\033[1;32mOrion-Flink Connector installed into local Maven repository\033[0m\n"