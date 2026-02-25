#!/bin/sh
# wait-for-keycloak.sh: waits for Keycloak to be fully available
until curl -s http://keycloak:8080/realms/ledgerflow/.well-known/openid-configuration; do
  echo "Waiting for Keycloak to be ready..."
  sleep 2
done
echo "Keycloak is ready!"