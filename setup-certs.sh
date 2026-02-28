#!/bin/bash

# Setup script to copy certificates from Fabric network
# Run this from the rezo-backend directory

FABRIC_NETWORK_PATH="${1:-$HOME/fabric-network}"

echo "Copying certificates from: $FABRIC_NETWORK_PATH"

# Check if fabric network path exists
if [ ! -d "$FABRIC_NETWORK_PATH" ]; then
    echo "Error: Fabric network path not found: $FABRIC_NETWORK_PATH"
    echo "Usage: ./setup-certs.sh /path/to/fabric-network"
    exit 1
fi

# Create wallet directory if it doesn't exist
mkdir -p wallet

# Copy peer TLS certificate
TLS_CERT="$FABRIC_NETWORK_PATH/organizations/peerOrganizations/switchorg.example.com/peers/peer0.switchorg.example.com/tls/ca.crt"
if [ -f "$TLS_CERT" ]; then
    cp "$TLS_CERT" ./peer-tls-cert.pem
    echo "Copied peer TLS certificate"
else
    echo "Warning: Peer TLS certificate not found at $TLS_CERT"
fi

# Copy admin certificate
ADMIN_CERT="$FABRIC_NETWORK_PATH/organizations/peerOrganizations/switchorg.example.com/users/Admin@switchorg.example.com/msp/signcerts/cert.pem"
if [ -f "$ADMIN_CERT" ]; then
    cp "$ADMIN_CERT" ./wallet/cert.pem
    echo "Copied admin certificate"
else
    echo "Warning: Admin certificate not found at $ADMIN_CERT"
fi

# Copy admin private key (find the *_sk file)
KEYSTORE_DIR="$FABRIC_NETWORK_PATH/organizations/peerOrganizations/switchorg.example.com/users/Admin@switchorg.example.com/msp/keystore"
if [ -d "$KEYSTORE_DIR" ]; then
    KEY_FILE=$(ls "$KEYSTORE_DIR"/*_sk 2>/dev/null | head -1)
    if [ -n "$KEY_FILE" ]; then
        cp "$KEY_FILE" ./wallet/key.pem
        echo "Copied admin private key"
    else
        echo "Warning: No private key found in $KEYSTORE_DIR"
    fi
else
    echo "Warning: Keystore directory not found at $KEYSTORE_DIR"
fi

echo ""
echo "Setup complete. Files created:"
ls -la peer-tls-cert.pem wallet/cert.pem wallet/key.pem 2>/dev/null

echo ""
echo "Next steps:"
echo "1. npm install"
echo "2. npm start"
