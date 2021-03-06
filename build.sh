#!bin/bash

echo "==============================================================================="
echo "Building Oasis..."
echo "==============================================================================="
mvn clean install -DskipTests

echo "==============================================================================="
echo "Building Events API Docker Image..."
echo "==============================================================================="
cd services/events-api
docker build -t oasis/events-api .

cd ../..

echo "==============================================================================="
echo "Building Admin/Stats API Docker Image..."
echo "==============================================================================="
cd services/stats-api
docker build -t oasis/stats-api .

cd ../..

echo "==============================================================================="
echo "Building Engine Docker Image..."
echo "==============================================================================="
cd engine
docker build -t oasis/engine .

cd ..

mkdir -p .tmpdata/redis
mkdir -p .tmpdata/rabbit

echo "==============================================================================="
echo "Starting Oasis..."
echo "==============================================================================="
docker-compose up



