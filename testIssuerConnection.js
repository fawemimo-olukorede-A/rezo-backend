/**
 * Test Issuer Connection
 * Verifies REZO can connect to the Issuer host
 */

const net = require('net');

const ISSUER_HOST = process.env.ISSUER_HOST || '172.26.42.206';
const ISSUER_PORT = process.env.ISSUER_PORT || 4534;
const TIMEOUT = 10000;  // 10 seconds

console.log('=============================================');
console.log('  REZO - Issuer Connection Test');
console.log('=============================================');
console.log(`Target: ${ISSUER_HOST}:${ISSUER_PORT}`);
console.log(`Timeout: ${TIMEOUT / 1000} seconds`);
console.log('---------------------------------------------');
console.log('Testing connection...\n');

const startTime = Date.now();
const socket = new net.Socket();

socket.setTimeout(TIMEOUT);

socket.connect(ISSUER_PORT, ISSUER_HOST, () => {
    const elapsed = Date.now() - startTime;
    console.log('=============================================');
    console.log('  CONNECTION SUCCESSFUL');
    console.log('=============================================');
    console.log(`Connected to ${ISSUER_HOST}:${ISSUER_PORT}`);
    console.log(`Time: ${elapsed}ms`);
    console.log('');
    console.log('The Issuer is reachable from this machine.');
    console.log('You can now run rezoMiddleware.js');
    console.log('=============================================');
    socket.destroy();
    process.exit(0);
});

socket.on('timeout', () => {
    console.log('=============================================');
    console.log('  CONNECTION TIMEOUT');
    console.log('=============================================');
    console.log(`Could not connect to ${ISSUER_HOST}:${ISSUER_PORT}`);
    console.log(`Timed out after ${TIMEOUT / 1000} seconds`);
    console.log('');
    console.log('Possible causes:');
    console.log('  - Issuer server is not running');
    console.log('  - Firewall blocking the connection');
    console.log('  - Wrong IP or port');
    console.log('  - Network routing issue');
    console.log('=============================================');
    socket.destroy();
    process.exit(1);
});

socket.on('error', (err) => {
    const elapsed = Date.now() - startTime;
    console.log('=============================================');
    console.log('  CONNECTION FAILED');
    console.log('=============================================');
    console.log(`Error: ${err.message}`);
    console.log(`Code: ${err.code}`);
    console.log(`Time: ${elapsed}ms`);
    console.log('');

    if (err.code === 'ECONNREFUSED') {
        console.log('The connection was refused.');
        console.log('The Issuer may not be listening on this port.');
    } else if (err.code === 'EHOSTUNREACH') {
        console.log('The host is unreachable.');
        console.log('Check network connectivity and routing.');
    } else if (err.code === 'ENETUNREACH') {
        console.log('Network is unreachable.');
        console.log('Check your network configuration.');
    }

    console.log('=============================================');
    socket.destroy();
    process.exit(1);
});
