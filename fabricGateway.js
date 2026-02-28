const { connect, signers } = require('@hyperledger/fabric-gateway');
const grpc = require('@grpc/grpc-js');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// Configuration - adjust these for your environment
const config = {
    mspId: process.env.MSP_ID || 'switchorgMSP',
    peerEndpoint: process.env.PEER_ENDPOINT || 'localhost:7051',
    peerHostAlias: process.env.PEER_HOST_ALIAS || 'peer0.switchorg.example.com',
    tlsCertPath: process.env.TLS_CERT_PATH || path.resolve(__dirname, 'peer-tls-cert.pem'),
    certPath: process.env.CERT_PATH || path.resolve(__dirname, 'wallet', 'cert.pem'),
    keyPath: process.env.KEY_PATH || path.resolve(__dirname, 'wallet', 'key.pem'),
};

/**
 * Create a gRPC client connection to the Fabric peer
 */
function createGrpcClient() {
    const tlsCert = fs.readFileSync(config.tlsCertPath);

    const tlsCredentials = grpc.credentials.createSsl(tlsCert);

    return new grpc.Client(
        config.peerEndpoint,
        tlsCredentials,
        {
            'grpc.ssl_target_name_override': config.peerHostAlias,
        }
    );
}

/**
 * Create identity object from certificate
 */
function createIdentity() {
    const cert = fs.readFileSync(config.certPath);
    return {
        mspId: config.mspId,
        credentials: cert,
    };
}

/**
 * Create signer from private key
 */
function createSigner() {
    const keyPem = fs.readFileSync(config.keyPath);
    const privateKey = crypto.createPrivateKey(keyPem);
    return signers.newPrivateKeySigner(privateKey);
}

/**
 * Create and return a Fabric Gateway connection
 */
async function createGateway() {
    const client = createGrpcClient();
    const identity = createIdentity();
    const signer = createSigner();

    const gateway = connect({
        client,
        identity,
        signer,
        // Enable service discovery to find endorsing peers automatically
        discovery: true,
        // Default timeouts for different operations
        evaluateOptions: () => ({ deadline: Date.now() + 5000 }), // 5 seconds
        endorseOptions: () => ({ deadline: Date.now() + 30000 }), // 30 seconds for discovery
        submitOptions: () => ({ deadline: Date.now() + 5000 }), // 5 seconds
        commitStatusOptions: () => ({ deadline: Date.now() + 60000 }), // 60 seconds
    });

    return { gateway, client };
}

/**
 * Close gateway and gRPC client connections
 */
async function closeGateway(gateway, client) {
    gateway.close();
    client.close();
}

module.exports = {
    createGateway,
    closeGateway,
    config,
};
