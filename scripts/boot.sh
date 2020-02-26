#!/bin/sh

# Stop and remove existing container
echo "Cleaning up existing containers...";
docker stop swift-tds-inst > /dev/null 2>&1 && docker rm swift-tds-inst > /dev/null 2>&1;
echo "Done.\n";

# Pull image
echo "Pulling latest SQL Server for Linux image...";
docker pull mcr.microsoft.com/mssql/server:2019-GA-ubuntu-16.04;
echo "Done.\n";

# Run docker container as root
echo "Starting sql server container...";
docker run --user=root -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=<YourStrong@Passw0rd>" \
-p 1433:1433 --name swift-tds-inst \
-d mcr.microsoft.com/mssql/server:2019-GA-ubuntu-16.04 > /dev/null 2>&1;
echo "Done.\n";

# Copy self-signed certificate and private key
echo "Copying SSL cert and key...";
docker cp ./certificate.pem swift-tds-inst:/etc/ssl/certs/mssql.pem && docker cp ./key.pem swift-tds-inst:/etc/ssl/private/mssql.key;
echo "Done.\n";

# Set path to self-signed certificate and key
docker exec swift-tds-inst /opt/mssql/bin/mssql-conf set network.tlscert /etc/ssl/certs/mssql.pem > /dev/null && \
docker exec swift-tds-inst /opt/mssql/bin/mssql-conf set network.tlskey /etc/ssl/private/mssql.key > /dev/null;

# Restart container
echo "More container prep...";
docker restart swift-tds-inst > /dev/null 2>&1;
echo "Done.\n";
echo "Ready for testing. SQL Server instance running on port 1433.";
