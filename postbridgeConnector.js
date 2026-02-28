/**
 * Postilion PostBridge Connector
 * Connects to Postilion switch via PostBridge interface
 *
 * PostBridge uses:
 * - TCP/IP connection
 * - 2-byte binary length header (network byte order / big-endian)
 * - ISO 8583:1987 message format
 * - Binary bitmap (8 or 16 bytes)
 */

const net = require('net');
const axios = require('axios');

// ============= CONFIGURATION =============
const SWITCH_HOST = process.env.SWITCH_HOST || '192.168.1.100';
const SWITCH_PORT = process.env.SWITCH_PORT || 5000;
const REZO_API_URL = process.env.REZO_API_URL || 'http://localhost:3000';

// PostBridge connection settings
const RECONNECT_INTERVAL = 5000;
const HEARTBEAT_INTERVAL = 30000;  // Send echo every 30 seconds
const SIGN_ON_REQUIRED = true;      // Set to true if switch requires sign-on

// Your institution identification (update these)
const INSTITUTION_ID = process.env.INSTITUTION_ID || '000000000001';  // Field 32/33
const TERMINAL_ID = process.env.TERMINAL_ID || 'REZO0001';            // Field 41

// ============= ISO 8583 FIELD DEFINITIONS (Postilion Format) =============

const FIELD_SPECS = {
    // Field definitions: [type, maxLength, lengthDigits]
    // type: 'fixed', 'llvar', 'lllvar', 'binary'
    2:   ['llvar', 19, 2],     // PAN
    3:   ['fixed', 6, 0],      // Processing Code
    4:   ['fixed', 12, 0],     // Amount, Transaction
    7:   ['fixed', 10, 0],     // Transmission Date & Time
    11:  ['fixed', 6, 0],      // STAN
    12:  ['fixed', 6, 0],      // Time, Local Transaction
    13:  ['fixed', 4, 0],      // Date, Local Transaction
    14:  ['fixed', 4, 0],      // Date, Expiration
    18:  ['fixed', 4, 0],      // Merchant Type
    22:  ['fixed', 3, 0],      // POS Entry Mode
    23:  ['fixed', 3, 0],      // Card Sequence Number
    25:  ['fixed', 2, 0],      // POS Condition Code
    26:  ['fixed', 2, 0],      // POS PIN Capture Code
    32:  ['llvar', 11, 2],     // Acquiring Institution ID
    33:  ['llvar', 11, 2],     // Forwarding Institution ID
    35:  ['llvar', 37, 2],     // Track 2 Data
    37:  ['fixed', 12, 0],     // Retrieval Reference Number
    38:  ['fixed', 6, 0],      // Authorization ID Response
    39:  ['fixed', 2, 0],      // Response Code
    41:  ['fixed', 8, 0],      // Card Acceptor Terminal ID
    42:  ['fixed', 15, 0],     // Card Acceptor ID Code
    43:  ['fixed', 40, 0],     // Card Acceptor Name/Location
    49:  ['fixed', 3, 0],      // Currency Code, Transaction
    52:  ['binary', 8, 0],     // PIN Data
    53:  ['fixed', 16, 0],     // Security Related Control Info
    54:  ['lllvar', 120, 3],   // Additional Amounts
    55:  ['lllvar', 999, 3],   // ICC Data (EMV)
    70:  ['fixed', 3, 0],      // Network Management Info Code
    90:  ['fixed', 42, 0],     // Original Data Elements
    95:  ['fixed', 42, 0],     // Replacement Amounts
    100: ['llvar', 11, 2],     // Receiving Institution ID
    102: ['llvar', 28, 2],     // Account ID 1
    103: ['llvar', 28, 2],     // Account ID 2
    123: ['lllvar', 15, 3],    // POS Data Code
    127: ['lllvar', 999, 3],   // Private Data (Postilion specific)
    128: ['binary', 8, 0],     // MAC
};

const PROCESSING_CODES = {
    '00': 'PURCHASE',
    '01': 'WITHDRAWAL',
    '02': 'WITHDRAWAL',  // Debit adjustment
    '09': 'PURCHASE',    // Purchase with cashback
    '20': 'REFUND',
    '21': 'REFUND',      // Deposit
    '30': 'BALANCE_INQUIRY',
    '31': 'BALANCE_INQUIRY',
    '40': 'TRANSFER',
    '50': 'PAYMENT',     // Bill payment
};

const RESPONSE_CODES = {
    '00': 'Approved',
    '01': 'Refer to issuer',
    '03': 'Invalid merchant',
    '05': 'Do not honor',
    '12': 'Invalid transaction',
    '13': 'Invalid amount',
    '14': 'Invalid card number',
    '30': 'Format error',
    '41': 'Lost card',
    '43': 'Stolen card',
    '51': 'Insufficient funds',
    '54': 'Expired card',
    '55': 'Incorrect PIN',
    '61': 'Exceeds withdrawal limit',
    '91': 'Issuer unavailable',
    '96': 'System malfunction',
};

// ============= MESSAGE BUILDING =============

/**
 * Build ISO 8583 message for Postilion
 */
function buildMessage(mti, fields) {
    const parts = [];

    // MTI (4 bytes ASCII)
    parts.push(Buffer.from(mti, 'ascii'));

    // Calculate bitmap
    const bitmap = calculateBitmap(Object.keys(fields).map(Number));
    parts.push(bitmap);

    // Add fields in order
    const sortedFields = Object.keys(fields).map(Number).sort((a, b) => a - b);

    for (const fieldNum of sortedFields) {
        const value = fields[fieldNum];
        const spec = FIELD_SPECS[fieldNum];

        if (!spec) {
            console.warn(`[PostBridge] Unknown field ${fieldNum}, skipping`);
            continue;
        }

        const [type, maxLen, lenDigits] = spec;
        let fieldData;

        if (type === 'fixed') {
            // Pad or truncate to exact length
            fieldData = Buffer.from(value.toString().padEnd(maxLen, ' ').substring(0, maxLen), 'ascii');
        } else if (type === 'llvar' || type === 'lllvar') {
            const dataStr = value.toString().substring(0, maxLen);
            const lenStr = dataStr.length.toString().padStart(lenDigits, '0');
            fieldData = Buffer.from(lenStr + dataStr, 'ascii');
        } else if (type === 'binary') {
            fieldData = Buffer.isBuffer(value) ? value : Buffer.from(value, 'hex');
        }

        if (fieldData) {
            parts.push(fieldData);
        }
    }

    // Combine all parts
    const messageBody = Buffer.concat(parts);

    // Add 2-byte length header (big-endian)
    const lengthHeader = Buffer.alloc(2);
    lengthHeader.writeUInt16BE(messageBody.length, 0);

    return Buffer.concat([lengthHeader, messageBody]);
}

/**
 * Calculate binary bitmap
 */
function calculateBitmap(fields) {
    const hasSecondary = fields.some(f => f > 64);
    const bitmapLength = hasSecondary ? 16 : 8;
    const bitmap = Buffer.alloc(bitmapLength);

    // Set bit 1 if secondary bitmap present
    if (hasSecondary) {
        bitmap[0] |= 0x80;
    }

    for (const field of fields) {
        if (field < 1 || field > 128) continue;

        const byteIndex = Math.floor((field - 1) / 8);
        const bitIndex = 7 - ((field - 1) % 8);
        bitmap[byteIndex] |= (1 << bitIndex);
    }

    return bitmap;
}

/**
 * Build Sign-On message (0800)
 */
function buildSignOn() {
    const now = new Date();
    const fields = {
        7: formatDateTime(now),           // Transmission date/time
        11: generateSTAN(),               // STAN
        70: '001',                         // Network Management Info Code (001 = Sign-on)
    };
    return buildMessage('0800', fields);
}

/**
 * Build Echo/Heartbeat message (0800)
 */
function buildEcho() {
    const now = new Date();
    const fields = {
        7: formatDateTime(now),
        11: generateSTAN(),
        70: '301',                         // Network Management Info Code (301 = Echo)
    };
    return buildMessage('0800', fields);
}

/**
 * Build Sign-Off message (0800)
 */
function buildSignOff() {
    const now = new Date();
    const fields = {
        7: formatDateTime(now),
        11: generateSTAN(),
        70: '002',                         // Network Management Info Code (002 = Sign-off)
    };
    return buildMessage('0800', fields);
}

// ============= MESSAGE PARSING =============

/**
 * Parse Postilion ISO 8583 message
 */
function parseMessage(buffer) {
    const message = { fields: {} };
    let offset = 0;

    try {
        // MTI (4 bytes)
        message.mti = buffer.toString('ascii', offset, offset + 4);
        offset += 4;

        // Primary bitmap (8 bytes binary)
        const primaryBitmap = buffer.slice(offset, offset + 8);
        offset += 8;

        // Check for secondary bitmap (bit 1 of primary)
        let fullBitmap = primaryBitmap;
        if (primaryBitmap[0] & 0x80) {
            const secondaryBitmap = buffer.slice(offset, offset + 8);
            offset += 8;
            fullBitmap = Buffer.concat([primaryBitmap, secondaryBitmap]);
        }

        // Parse fields based on bitmap
        for (let fieldNum = 2; fieldNum <= (fullBitmap.length * 8); fieldNum++) {
            if (!isBitSet(fullBitmap, fieldNum)) continue;

            const spec = FIELD_SPECS[fieldNum];
            if (!spec) {
                console.warn(`[PostBridge] Unknown field ${fieldNum} in message`);
                continue;
            }

            const [type, maxLen, lenDigits] = spec;
            let fieldValue;
            let fieldLength = maxLen;

            if (type === 'llvar') {
                fieldLength = parseInt(buffer.toString('ascii', offset, offset + 2));
                offset += 2;
            } else if (type === 'lllvar') {
                fieldLength = parseInt(buffer.toString('ascii', offset, offset + 3));
                offset += 3;
            }

            if (type === 'binary') {
                fieldValue = buffer.slice(offset, offset + fieldLength).toString('hex');
            } else {
                fieldValue = buffer.toString('ascii', offset, offset + fieldLength);
            }

            offset += fieldLength;
            message.fields[fieldNum] = fieldValue.trim();
        }

    } catch (err) {
        console.error('[PostBridge] Parse error:', err.message);
    }

    return message;
}

/**
 * Check if bit is set in binary bitmap
 */
function isBitSet(bitmap, bitNum) {
    if (bitNum < 1 || bitNum > bitmap.length * 8) return false;
    const byteIndex = Math.floor((bitNum - 1) / 8);
    const bitIndex = 7 - ((bitNum - 1) % 8);
    return (bitmap[byteIndex] & (1 << bitIndex)) !== 0;
}

// ============= UTILITIES =============

let stanCounter = 0;

function generateSTAN() {
    stanCounter = (stanCounter + 1) % 1000000;
    return stanCounter.toString().padStart(6, '0');
}

function formatDateTime(date) {
    const mm = (date.getUTCMonth() + 1).toString().padStart(2, '0');
    const dd = date.getUTCDate().toString().padStart(2, '0');
    const hh = date.getUTCHours().toString().padStart(2, '0');
    const mi = date.getUTCMinutes().toString().padStart(2, '0');
    const ss = date.getUTCSeconds().toString().padStart(2, '0');
    return `${mm}${dd}${hh}${mi}${ss}`;
}

function maskPAN(pan) {
    if (!pan || pan.length < 13) return pan;
    return pan.substring(0, 6) + '****' + pan.substring(pan.length - 4);
}

// ============= TRANSACTION PROCESSING =============

/**
 * Convert Postilion message to blockchain transaction
 */
function toBlockchainTx(msg) {
    const f = msg.fields;
    const processingCode = (f[3] || '000000').substring(0, 2);
    const txType = PROCESSING_CODES[processingCode] || 'PURCHASE';
    const amount = parseInt(f[4] || '0') / 100;  // Convert minor units to Naira

    return {
        rrn: f[37] || '',
        stan: f[11] || '',
        maskedPan: maskPAN(f[2]),
        acquirerCode: f[32] || '',
        issuerCode: f[100] || '',
        terminalId: f[41] || '',
        merchantId: f[42] || '',
        merchantName: f[43] || '',
        amount: amount,
        currency: 'NGN',
        txType: txType,
        authCode: f[38] || '',
        responseCode: f[39] || '',
        metadata: JSON.stringify({
            mti: msg.mti,
            processingCode: f[3],
            transmissionDateTime: f[7],
            posEntryMode: f[22],
            responseDescription: RESPONSE_CODES[f[39]] || 'Unknown',
        }),
    };
}

/**
 * Log transaction to blockchain
 */
async function logToBlockchain(tx) {
    try {
        const response = await axios.post(`${REZO_API_URL}/transactions`, tx);
        console.log(`[Blockchain] Logged: RRN=${tx.rrn}, ${tx.txType}, ₦${tx.amount.toLocaleString()}`);
        return response.data;
    } catch (err) {
        console.error(`[Blockchain] Error: ${err.message}`);
    }
}

// ============= CONNECTION MANAGEMENT =============

let client = null;
let reconnectTimer = null;
let heartbeatTimer = null;
let isSignedOn = false;

/**
 * Connect to Postilion PostBridge
 */
function connect() {
    console.log(`[PostBridge] Connecting to ${SWITCH_HOST}:${SWITCH_PORT}...`);

    client = new net.Socket();

    client.connect(SWITCH_PORT, SWITCH_HOST, () => {
        console.log(`[PostBridge] Connected!`);

        if (reconnectTimer) {
            clearTimeout(reconnectTimer);
            reconnectTimer = null;
        }

        // Send Sign-On if required
        if (SIGN_ON_REQUIRED) {
            sendSignOn();
        } else {
            isSignedOn = true;
            startHeartbeat();
        }
    });

    let buffer = Buffer.alloc(0);

    client.on('data', async (data) => {
        buffer = Buffer.concat([buffer, data]);

        // Process complete messages (2-byte length header)
        while (buffer.length >= 2) {
            const msgLen = buffer.readUInt16BE(0);

            if (buffer.length >= msgLen + 2) {
                const message = buffer.slice(2, msgLen + 2);
                buffer = buffer.slice(msgLen + 2);
                await processMessage(message);
            } else {
                break;
            }
        }
    });

    client.on('close', () => {
        console.log('[PostBridge] Connection closed');
        isSignedOn = false;
        stopHeartbeat();
        scheduleReconnect();
    });

    client.on('error', (err) => {
        console.error(`[PostBridge] Error: ${err.message}`);
    });
}

/**
 * Send Sign-On message
 */
function sendSignOn() {
    console.log('[PostBridge] Sending Sign-On (0800)...');
    const signOn = buildSignOn();
    client.write(signOn);
}

/**
 * Process received message
 */
async function processMessage(buffer) {
    const msg = parseMessage(buffer);

    console.log(`[PostBridge] Received: MTI=${msg.mti}, RRN=${msg.fields[37] || 'N/A'}`);

    // Handle network management messages
    if (msg.mti === '0810') {
        const responseCode = msg.fields[39];
        const networkCode = msg.fields[70];

        if (networkCode === '001' || networkCode === '301') {
            if (responseCode === '00') {
                console.log('[PostBridge] Sign-On/Echo successful');
                if (!isSignedOn) {
                    isSignedOn = true;
                    startHeartbeat();
                }
            } else {
                console.error(`[PostBridge] Sign-On/Echo failed: ${responseCode}`);
            }
        }
        return;
    }

    // Handle financial transactions (0200, 0210, 0220, 0230)
    if (['0200', '0210', '0220', '0230'].includes(msg.mti)) {
        const responseCode = msg.fields[39];

        // Log all transactions, or only approved ones
        if (!responseCode || responseCode === '00') {
            const tx = toBlockchainTx(msg);
            await logToBlockchain(tx);
        } else {
            console.log(`[PostBridge] Declined: RRN=${msg.fields[37]}, Code=${responseCode} (${RESPONSE_CODES[responseCode] || 'Unknown'})`);
        }
    }

    // Handle reversals (0400, 0410, 0420, 0430)
    if (['0400', '0410', '0420', '0430'].includes(msg.mti)) {
        console.log(`[PostBridge] Reversal: RRN=${msg.fields[37]}`);
        const tx = toBlockchainTx(msg);
        tx.txType = 'REVERSAL';
        await logToBlockchain(tx);
    }
}

/**
 * Start heartbeat
 */
function startHeartbeat() {
    heartbeatTimer = setInterval(() => {
        if (client && !client.destroyed && isSignedOn) {
            console.log('[PostBridge] Sending Echo...');
            const echo = buildEcho();
            client.write(echo);
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
 * Schedule reconnection
 */
function scheduleReconnect() {
    if (!reconnectTimer) {
        console.log(`[PostBridge] Reconnecting in ${RECONNECT_INTERVAL / 1000}s...`);
        reconnectTimer = setTimeout(() => {
            reconnectTimer = null;
            connect();
        }, RECONNECT_INTERVAL);
    }
}

/**
 * Graceful shutdown
 */
function shutdown() {
    console.log('[PostBridge] Shutting down...');

    if (isSignedOn && client && !client.destroyed) {
        console.log('[PostBridge] Sending Sign-Off...');
        const signOff = buildSignOff();
        client.write(signOff);
    }

    stopHeartbeat();
    if (reconnectTimer) clearTimeout(reconnectTimer);

    setTimeout(() => {
        if (client) client.destroy();
        process.exit(0);
    }, 1000);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// ============= START =============

console.log('=====================================================');
console.log('   REZO - Postilion PostBridge Connector');
console.log('   ISO 8583 to Hyperledger Fabric Bridge');
console.log('=====================================================');
console.log(`Switch:        ${SWITCH_HOST}:${SWITCH_PORT}`);
console.log(`Blockchain:    ${REZO_API_URL}`);
console.log(`Sign-On:       ${SIGN_ON_REQUIRED ? 'Enabled' : 'Disabled'}`);
console.log(`Heartbeat:     ${HEARTBEAT_INTERVAL / 1000}s`);
console.log('=====================================================');

connect();
