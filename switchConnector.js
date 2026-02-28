/**
 * Switch Connector - Connect to existing payment switch
 * Receives ISO 8583 messages and logs to blockchain
 */

const net = require('net');
const axios = require('axios');

// ============= CONFIGURATION =============
// Update these with your switch details
const SWITCH_HOST = process.env.SWITCH_HOST || '192.168.1.100';  // Switch IP
const SWITCH_PORT = process.env.SWITCH_PORT || 5000;              // Switch Port
const REZO_API_URL = process.env.REZO_API_URL || 'http://localhost:3000';

// Reconnection settings
const RECONNECT_INTERVAL = 5000;  // 5 seconds
const HEARTBEAT_INTERVAL = 30000; // 30 seconds

// ============= ISO 8583 PARSING =============

const PROCESSING_CODES = {
    '00': 'PURCHASE',
    '01': 'WITHDRAWAL',
    '20': 'REFUND',
    '31': 'BALANCE_INQUIRY',
    '40': 'TRANSFER',
};

const CURRENCY_CODES = {
    '566': 'NGN',
    '840': 'USD',
};

/**
 * Simple ISO 8583 parser (adjust based on switch's specific format)
 */
function parseISO8583(buffer) {
    const message = {};

    try {
        // Most switches send length-prefixed messages
        // Format varies - adjust based on your switch documentation

        // Example: 4-byte length header + message
        let offset = 0;

        // MTI (Message Type Indicator) - 4 bytes
        message.mti = buffer.toString('ascii', offset, offset + 4);
        offset += 4;

        // Primary Bitmap - 8 bytes (64 bits) or 16 bytes hex
        const bitmapLength = 16; // Adjust based on switch
        message.bitmap = buffer.toString('hex', offset, offset + bitmapLength);
        offset += bitmapLength;

        // Parse fields based on bitmap
        // This is simplified - real parsing depends on switch format

        // Field 2 - PAN (variable length, LLVAR)
        if (isBitSet(message.bitmap, 2)) {
            const len = parseInt(buffer.toString('ascii', offset, offset + 2));
            offset += 2;
            message.pan = buffer.toString('ascii', offset, offset + len);
            offset += len;
        }

        // Field 3 - Processing Code (6 fixed)
        if (isBitSet(message.bitmap, 3)) {
            message.processingCode = buffer.toString('ascii', offset, offset + 6);
            offset += 6;
        }

        // Field 4 - Amount (12 fixed)
        if (isBitSet(message.bitmap, 4)) {
            message.amount = buffer.toString('ascii', offset, offset + 12);
            offset += 12;
        }

        // Field 11 - STAN (6 fixed)
        if (isBitSet(message.bitmap, 11)) {
            message.stan = buffer.toString('ascii', offset, offset + 6);
            offset += 6;
        }

        // Field 32 - Acquiring Institution (LLVAR)
        if (isBitSet(message.bitmap, 32)) {
            const len = parseInt(buffer.toString('ascii', offset, offset + 2));
            offset += 2;
            message.acquirer = buffer.toString('ascii', offset, offset + len);
            offset += len;
        }

        // Field 37 - RRN (12 fixed)
        if (isBitSet(message.bitmap, 37)) {
            message.rrn = buffer.toString('ascii', offset, offset + 12);
            offset += 12;
        }

        // Field 38 - Auth Code (6 fixed)
        if (isBitSet(message.bitmap, 38)) {
            message.authCode = buffer.toString('ascii', offset, offset + 6);
            offset += 6;
        }

        // Field 39 - Response Code (2 fixed)
        if (isBitSet(message.bitmap, 39)) {
            message.responseCode = buffer.toString('ascii', offset, offset + 2);
            offset += 2;
        }

        // Field 41 - Terminal ID (8 fixed)
        if (isBitSet(message.bitmap, 41)) {
            message.terminalId = buffer.toString('ascii', offset, offset + 8);
            offset += 8;
        }

        // Field 42 - Merchant ID (15 fixed)
        if (isBitSet(message.bitmap, 42)) {
            message.merchantId = buffer.toString('ascii', offset, offset + 15);
            offset += 15;
        }

        // Field 43 - Merchant Name (40 fixed)
        if (isBitSet(message.bitmap, 43)) {
            message.merchantName = buffer.toString('ascii', offset, offset + 40);
            offset += 40;
        }

        // Field 100 - Receiving Institution / Issuer (LLVAR)
        if (isBitSet(message.bitmap, 100)) {
            const len = parseInt(buffer.toString('ascii', offset, offset + 2));
            offset += 2;
            message.issuer = buffer.toString('ascii', offset, offset + len);
            offset += len;
        }

    } catch (err) {
        console.error('[Parser] Error parsing ISO 8583:', err.message);
    }

    return message;
}

/**
 * Check if bit is set in hex bitmap
 */
function isBitSet(bitmapHex, bitNumber) {
    try {
        const bitmap = BigInt('0x' + bitmapHex);
        const bitPosition = BigInt(1) << BigInt(64 - bitNumber);
        return (bitmap & bitPosition) !== BigInt(0);
    } catch {
        return false;
    }
}

/**
 * Mask PAN for security
 */
function maskPAN(pan) {
    if (!pan || pan.length < 13) return pan;
    return pan.substring(0, 6) + '****' + pan.substring(pan.length - 4);
}

/**
 * Convert ISO message to blockchain transaction
 */
function toBlockchainTx(iso) {
    const processingCode = iso.processingCode?.substring(0, 2) || '00';
    const txType = PROCESSING_CODES[processingCode] || 'PURCHASE';
    const amount = parseInt(iso.amount || '0') / 100; // Convert kobo to Naira

    return {
        rrn: (iso.rrn || '').trim(),
        stan: (iso.stan || '').trim(),
        maskedPan: maskPAN(iso.pan),
        acquirerCode: (iso.acquirer || '').trim(),
        issuerCode: (iso.issuer || '').trim(),
        terminalId: (iso.terminalId || '').trim(),
        merchantId: (iso.merchantId || '').trim(),
        merchantName: (iso.merchantName || '').trim(),
        amount: amount,
        currency: 'NGN',
        txType: txType,
        authCode: (iso.authCode || '').trim(),
        responseCode: (iso.responseCode || '').trim(),
        metadata: JSON.stringify({ mti: iso.mti, processingCode: iso.processingCode }),
    };
}

/**
 * Log transaction to blockchain
 */
async function logToBlockchain(tx) {
    try {
        const response = await axios.post(`${REZO_API_URL}/transactions`, tx);
        console.log(`[Blockchain] Logged: RRN=${tx.rrn}, Amount=₦${tx.amount.toLocaleString()}`);
        return response.data;
    } catch (err) {
        console.error(`[Blockchain] Error: ${err.message}`);
    }
}

// ============= SWITCH CONNECTION =============

let client = null;
let reconnectTimer = null;
let heartbeatTimer = null;

/**
 * Connect to the payment switch
 */
function connectToSwitch() {
    console.log(`[Switch] Connecting to ${SWITCH_HOST}:${SWITCH_PORT}...`);

    client = new net.Socket();

    client.connect(SWITCH_PORT, SWITCH_HOST, () => {
        console.log(`[Switch] Connected to ${SWITCH_HOST}:${SWITCH_PORT}`);

        // Clear reconnect timer
        if (reconnectTimer) {
            clearTimeout(reconnectTimer);
            reconnectTimer = null;
        }

        // Start heartbeat
        startHeartbeat();

        // Send sign-on message if required by switch
        // sendSignOn();
    });

    let buffer = Buffer.alloc(0);

    client.on('data', async (data) => {
        buffer = Buffer.concat([buffer, data]);

        // Process complete messages
        // Adjust based on switch's message framing (length header, delimiter, etc.)
        while (buffer.length >= 4) {
            // Assume 4-byte length header (adjust as needed)
            const msgLen = parseInt(buffer.toString('ascii', 0, 4));

            if (isNaN(msgLen) || msgLen <= 0) {
                // Try without length header
                await processMessage(buffer);
                buffer = Buffer.alloc(0);
                break;
            }

            if (buffer.length >= msgLen + 4) {
                const message = buffer.slice(4, msgLen + 4);
                buffer = buffer.slice(msgLen + 4);
                await processMessage(message);
            } else {
                break; // Wait for more data
            }
        }
    });

    client.on('close', () => {
        console.log('[Switch] Connection closed');
        stopHeartbeat();
        scheduleReconnect();
    });

    client.on('error', (err) => {
        console.error(`[Switch] Error: ${err.message}`);
        stopHeartbeat();
        scheduleReconnect();
    });
}

/**
 * Process received message
 */
async function processMessage(buffer) {
    console.log(`[Switch] Received ${buffer.length} bytes`);

    const iso = parseISO8583(buffer);
    console.log(`[Switch] MTI: ${iso.mti}, RRN: ${iso.rrn}, Amount: ${iso.amount}`);

    // Only log approved financial transactions
    if (['0200', '0210', '0220'].includes(iso.mti)) {
        if (!iso.responseCode || iso.responseCode === '00') {
            const tx = toBlockchainTx(iso);
            await logToBlockchain(tx);
        } else {
            console.log(`[Switch] Skipping declined: ResponseCode=${iso.responseCode}`);
        }
    }
}

/**
 * Schedule reconnection
 */
function scheduleReconnect() {
    if (!reconnectTimer) {
        console.log(`[Switch] Reconnecting in ${RECONNECT_INTERVAL/1000}s...`);
        reconnectTimer = setTimeout(() => {
            reconnectTimer = null;
            connectToSwitch();
        }, RECONNECT_INTERVAL);
    }
}

/**
 * Start heartbeat/keep-alive
 */
function startHeartbeat() {
    heartbeatTimer = setInterval(() => {
        if (client && !client.destroyed) {
            // Send network management message (0800) if required
            // client.write(buildHeartbeat());
            console.log('[Switch] Heartbeat - connection alive');
        }
    }, HEARTBEAT_INTERVAL);
}

/**
 * Stop heartbeat
 */
function stopHeartbeat() {
    if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
        heartbeatTimer = null;
    }
}

/**
 * Graceful shutdown
 */
function shutdown() {
    console.log('[Switch] Shutting down...');
    stopHeartbeat();
    if (reconnectTimer) clearTimeout(reconnectTimer);
    if (client) client.destroy();
    process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// ============= START =============

console.log('===========================================');
console.log('   REZO Switch Connector');
console.log('   ISO 8583 to Blockchain Bridge');
console.log('===========================================');
console.log(`Switch: ${SWITCH_HOST}:${SWITCH_PORT}`);
console.log(`Blockchain API: ${REZO_API_URL}`);
console.log('===========================================');

connectToSwitch();
