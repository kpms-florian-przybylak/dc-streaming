#!/bin/bash

# Abrufen aller Container-IDs, die mit "streaming" markiert sind
containers=$(docker ps -a | grep "streaming" | awk '{print $1}')

# Stoppe alle gefundenen Container
for container in $containers; do
    echo "Stopping container: $container"
    sudo docker stop $container
done

# Entferne alle gestoppten Container
for container in $containers; do
    echo "Removing container: $container"
    sudo docker rm $container
done

# Baue das Docker-Image neu
echo "Building new Docker image for dc-streaming"
sudo docker build -t dc-streaming .

# Erstelle und starte einen neuen Container aus dem neu gebauten Image
echo "Creating and starting new container from dc-streaming image"
container_id=$(sudo docker run -d -p 8000:8000 dc-streaming)

echo "New container created with ID: $container_id"

# Warte einen Moment, um dem Container Zeit zu geben, zu starten und Logs zu generieren
sleep 5

# Zeige die Logs des neu erstellten Containers
echo "Showing logs for the container: $container_id"
sudo docker logs $container_id

echo "All done."
