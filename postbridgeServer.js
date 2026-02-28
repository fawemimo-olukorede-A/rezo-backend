/**
 * REZO PostBridge Server
 * Listens for incoming ISO 8583 messages from Postilion
 *
 * Configure Postilion to connect to:
 *   IP:   172.26.40.36
 *   PORT: 5000
 */

const net = require('net');
const axios = require('axios');

// ============= CONFIGURATION =============
const LISTEN_PORT = process.env.LISTEN_PORT || 5000;        // Port Postilion connects to
const LISTEN_HOST = process.env.LISTEN_HOST || '0.0.0.0';   // Listen on all interfaces
const REZO_API_URL = process.env.REZO_API_URL || 'http://localhost:3000';

// Postilion-specific settings
const USE_ASCII_LENGTH_HEADER = process.env.ASCII_LENGTH || false;  // Set true if Postilion uses ASCII length
const HEADER_LENGTH = 2;  // 2-byte header (change to 4 if Postilion uses 4-byte)
const LOG_RAW_DATA = process.env.LOG_RAW || true;  // Set true to debug raw hex data

// ============= ISO 8583 FIELD DEFINITIONS =============

const FIELD_SPECS = {
    2:   ['llvar', 19, 2],     // PAN
    3:   ['fixed', 6, 0],      // Processing Code
    4:   ['fixed', 12, 0],     // Amount
    7:   ['fixed', 10, 0],     // Transmission Date & Time
    11:  ['fixed', 6, 0],      // STAN
    12:  ['fixed', 6, 0],      // Local Time
    13:  ['fixed', 4, 0],      // Local Date
    14:  ['fixed', 4, 0],      // Expiry Date
    18:  ['fixed', 4, 0],      // Merchant Type
    22:  ['fixed', 3, 0],      // POS Entry Mode
    23:  ['fixed', 3, 0],      // Card Sequence Number
    25:  ['fixed', 2, 0],      // POS Condition Code
    32:  ['llvar', 11, 2],     // Acquiring Institution ID (Acquirer)
    33:  ['llvar', 11, 2],     // Forwarding Institution ID
    35:  ['llvar', 37, 2],     // Track 2 Data
    37:  ['fixed', 12, 0],     // RRN
    38:  ['fixed', 6, 0],      // Auth Code
    39:  ['fixed', 2, 0],      // Response Code
    41:  ['fixed', 8, 0],      // Terminal ID
    42:  ['fixed', 15, 0],     // Merchant ID
    43:  ['fixed', 40, 0],     // Merchant Name/Location
    49:  ['fixed', 3, 0],      // Currency Code
    52:  ['binary', 8, 0],     // PIN Data
    54:  ['lllvar', 120, 3],   // Additional Amounts
    55:  ['lllvar', 999, 3],   // ICC/EMV Data
    70:  ['fixed', 3, 0],      // Network Management Code
    90:  ['fixed', 42, 0],     // Original Data Elements
    100: ['llvar', 11, 2],     // Receiving Institution (Issuer)
    102: ['llvar', 28, 2],     // Account ID 1
    103: ['llvar', 28, 2],     // Account ID 2
    123: ['lllvar', 15, 3],    // POS Data Code
    127: ['lllvar', 999, 3],   // Private Data
    128: ['binary', 8, 0],     // MAC
};

const PROCESSING_CODES = {
    '00': 'PURCHASE',
    '01': 'WITHDRAWAL',
    '09': 'PURCHASE',        // Purchase with cashback
    '20': 'REFUND',
    '30': 'BALANCE_INQUIRY',
    '31': 'BALANCE_INQUIRY',
    '40': 'TRANSFER',
};

const RESPONSE_CODES = {
    '00': 'Approved',
    '01': 'Refer to issuer',
    '05': 'Do not honor',
    '12': 'Invalid transaction',
    '13': 'Invalid amount',
    '14': 'Invalid card number',
    '51': 'Insufficient funds',
    '54': 'Expired card',
    '55': 'Incorrect PIN',
    '91': 'Issuer unavailable',
    '96': 'System malfunction',
};

// ============= MESSAGE PARSING =============

function parseMessage(buffer) {
    const message = { fields: {} };
    let offset = 0;

    try {
        // MTI (4 bytes ASCII)
        message.mti = buffer.toString('ascii', offset, offset + 4);
        offset += 4;

        // Primary bitmap (8 bytes binary)
        const primaryBitmap = buffer.slice(offset, offset + 8);
        offset += 8;

        // Check for secondary bitmap
        let fullBitmap = primaryBitmap;
        if (primaryBitmap[0] & 0x80) {
            const secondaryBitmap = buffer.slice(offset, offset + 8);
            offset += 8;
            fullBitmap = Buffer.concat([primaryBitmap, secondaryBitmap]);
        }

        // Parse fields
        for (let fieldNum = 2; fieldNum <= fullBitmap.length * 8; fieldNum++) {
            if (!isBitSet(fullBitmap, fieldNum)) continue;

            const spec = FIELD_SPECS[fieldNum];
            if (!spec) continue;

            const [type, maxLen, lenDigits] = spec;
            let fieldLength = maxLen;

            if (type === 'llvar') {
                fieldLength = parseInt(buffer.toString('ascii', offset, offset + 2)) || 0;
                offset += 2;
            } else if (type === 'lllvar') {
                fieldLength = parseInt(buffer.toString('ascii', offset, offset + 3)) || 0;
                offset += 3;
            }

            let fieldValue;
            if (type === 'binary') {
                fieldValue = buffer.slice(offset, offset + fieldLength).toString('hex');
            } else {
                fieldValue = buffer.toString('ascii', offset, offset + fieldLength);
            }

            offset += fieldLength;
            message.fields[fieldNum] = fieldValue.trim();
        }
    } catch (err) {
        console.error('[Parser] Error:', err.message);
    }

    return message;
}

function isBitSet(bitmap, bitNum) {
    if (bitNum < 1 || bitNum > bitmap.length * 8) return false;
    const byteIndex = Math.floor((bitNum - 1) / 8);
    const bitIndex = 7 - ((bitNum - 1) % 8);
    return (bitmap[byteIndex] & (1 << bitIndex)) !== 0;
}

// ============= RESPONSE BUILDING =============

function buildResponse(mti, fields) {
    const parts = [];

    // Response MTI (increment by 10)
    const responseMTI = (parseInt(mti) + 10).toString().padStart(4, '0');
    parts.push(Buffer.from(responseMTI, 'ascii'));

    // Calculate bitmap
    const bitmap = calculateBitmap(Object.keys(fields).map(Number));
    parts.push(bitmap);

    // Add fields
    const sortedFields = Object.keys(fields).map(Number).sort((a, b) => a - b);
    for (const fieldNum of sortedFields) {
        const value = fields[fieldNum];
        const spec = FIELD_SPECS[fieldNum];
        if (!spec) continue;

        const [type, maxLen, lenDigits] = spec;
        let fieldData;

        if (type === 'fixed') {
            fieldData = Buffer.from(value.toString().padEnd(maxLen, ' ').substring(0, maxLen), 'ascii');
        } else if (type === 'llvar' || type === 'lllvar') {
            const dataStr = value.toString().substring(0, maxLen);
            const lenStr = dataStr.length.toString().padStart(lenDigits, '0');
            fieldData = Buffer.from(lenStr + dataStr, 'ascii');
        } else if (type === 'binary') {
            fieldData = Buffer.isBuffer(value) ? value : Buffer.from(value, 'hex');
        }

        if (fieldData) parts.push(fieldData);
    }

    const messageBody = Buffer.concat(parts);

    // 2-byte length header (big-endian)
    const lengthHeader = Buffer.alloc(2);
    lengthHeader.writeUInt16BE(messageBody.length, 0);

    return Buffer.concat([lengthHeader, messageBody]);
}

function calculateBitmap(fields) {
    const hasSecondary = fields.some(f => f > 64);
    const bitmapLength = hasSecondary ? 16 : 8;
    const bitmap = Buffer.alloc(bitmapLength);

    if (hasSecondary) bitmap[0] |= 0x80;

    for (const field of fields) {
        if (field < 1 || field > 128) continue;
        const byteIndex = Math.floor((field - 1) / 8);
        const bitIndex = 7 - ((field - 1) % 8);
        bitmap[byteIndex] |= (1 << bitIndex);
    }

    return bitmap;
}

// ============= UTILITIES =============

function maskPAN(pan) {
    if (!pan || pan.length < 13) return pan;
    return pan.substring(0, 6) + '****' + pan.substring(pan.length - 4);
}

function formatDateTime(date) {
    const mm = (date.getUTCMonth() + 1).toString().padStart(2, '0');
    const dd = date.getUTCDate().toString().padStart(2, '0');
    const hh = date.getUTCHours().toString().padStart(2, '0');
    const mi = date.getUTCMinutes().toString().padStart(2, '0');
    const ss = date.getUTCSeconds().toString().padStart(2, '0');
    return `${mm}${dd}${hh}${mi}${ss}`;
}

// ============= BLOCKCHAIN LOGGING =============

function toBlockchainTx(msg) {
    const f = msg.fields;
    const processingCode = (f[3] || '000000').substring(0, 2);
    const txType = PROCESSING_CODES[processingCode] || 'PURCHASE';
    const amount = parseInt(f[4] || '0') / 100;

    // Extract acquirer - try multiple fields
    // Field 32 = Acquiring Institution ID
    // Field 33 = Forwarding Institution ID (sometimes used as acquirer)
    // Field 42 = Card Acceptor ID (first 3-4 chars sometimes contain bank code)
    let acquirerCode = f[32] || f[33] || '';
    if (!acquirerCode || acquirerCode === '0' || acquirerCode.trim() === '') {
        // Try to extract from terminal ID or merchant ID
        acquirerCode = 'ISW';  // Default to Interswitch if not specified
    }
    acquirerCode = acquirerCode.trim();

    // Extract issuer - try multiple fields
    // Field 100 = Receiving Institution ID
    // Can also try to derive from PAN (BIN lookup)
    let issuerCode = f[100] || '';
    if (!issuerCode || issuerCode.trim() === '') {
        // Try to get issuer from PAN BIN (first 6 digits)
        const pan = f[2] || '';
        if (pan.length >= 6) {
            const bin = pan.substring(0, 6);
            issuerCode = getBankFromBIN(bin) || 'UNKNOWN';
        } else {
            issuerCode = 'UNKNOWN';
        }
    }
    issuerCode = issuerCode.trim();

    return {
        rrn: f[37] || '',
        stan: f[11] || '',
        maskedPan: maskPAN(f[2]),
        acquirerCode: acquirerCode,
        issuerCode: issuerCode,
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
            merchantType: f[18],
            posEntryMode: f[22],
            originalAcquirer: f[32],
            originalIssuer: f[100],
        }),
    };
}

/**
 * Get bank code from BIN (first 6 digits of PAN)
 * Nigerian bank BIN ranges
 */
function getBankFromBIN(bin) {
    const binRanges = {
        // Mastercard Nigeria ranges
        '539983': '058',  // GTBank
        '539941': '058',  // GTBank
        '544937': '044',  // Access Bank
        '544927': '044',  // Access Bank
        '531083': '057',  // Zenith Bank
        '531995': '057',  // Zenith Bank
        '519911': '033',  // UBA
        '519940': '033',  // UBA
        '530988': '011',  // First Bank
        '539117': '011',  // First Bank
        '530220': '070',  // Fidelity Bank
        '506105': '214',  // FCMB
        '506106': '214',  // FCMB
        '506107': '232',  // Sterling
        // Visa Nigeria ranges
        '405633': '058',  // GTBank Visa
        '408019': '044',  // Access Visa
        '428623': '057',  // Zenith Visa
        '466498': '033',  // UBA Visa
    };

    // Check exact match first
    if (binRanges[bin]) return binRanges[bin];

    // Check prefix matches (first 4-5 digits)
    for (const [prefix, bank] of Object.entries(binRanges)) {
        if (bin.startsWith(prefix.substring(0, 4))) {
            return bank;
        }
    }

    // Default mappings based on card type
    if (bin.startsWith('5')) return 'MASTERCARD';  // Mastercard
    if (bin.startsWith('4')) return 'VISA';        // Visa
    if (bin.startsWith('506')) return 'VERVE';     // Verve

    return null;
}

async function logToBlockchain(tx) {
    try {
        const response = await axios.post(`${REZO_API_URL}/transactions`, tx);
        console.log(`[Blockchain] ✓ Logged: RRN=${tx.rrn}, ${tx.txType}, ₦${tx.amount.toLocaleString()}, ${tx.acquirerCode}→${tx.issuerCode}`);
        return response.data;
    } catch (err) {
        console.error(`[Blockchain] ✗ Error: ${err.message}`);
    }
}

// ============= MESSAGE HANDLING =============

async function handleMessage(socket, buffer) {
    const msg = parseMessage(buffer);
    const f = msg.fields;

    console.log('─'.repeat(60));
    console.log(`[Received] MTI=${msg.mti} RRN=${f[37] || 'N/A'} Amount=${f[4] || '0'}`);

    // Handle Network Management (0800)
    if (msg.mti === '0800') {
        const networkCode = f[70];
        console.log(`[Network] Code=${networkCode} (${networkCode === '001' ? 'Sign-On' : networkCode === '301' ? 'Echo' : 'Other'})`);

        // Send response (0810)
        const response = buildResponse(msg.mti, {
            7: formatDateTime(new Date()),
            11: f[11],
            39: '00',  // Approved
            70: f[70],
        });
        socket.write(response);
        console.log(`[Sent] MTI=0810 Response=00 (Approved)`);
        return;
    }

    // Handle Financial Messages (0200, 0210, 0220)
    if (['0200', '0210', '0220'].includes(msg.mti)) {
        const responseCode = f[39] || '00';

        console.log(`[Transaction] ${PROCESSING_CODES[(f[3] || '').substring(0, 2)] || 'UNKNOWN'}`);
        console.log(`  PAN:      ${maskPAN(f[2])}`);
        console.log(`  Amount:   ₦${(parseInt(f[4] || '0') / 100).toLocaleString()}`);
        console.log(`  Acquirer: ${f[32] || 'N/A'}`);
        console.log(`  Issuer:   ${f[100] || 'N/A'}`);
        console.log(`  Terminal: ${f[41] || 'N/A'}`);
        console.log(`  Merchant: ${(f[43] || 'N/A').trim()}`);
        console.log(`  Response: ${responseCode} (${RESPONSE_CODES[responseCode] || 'Unknown'})`);

        // Log approved transactions to blockchain
        if (responseCode === '00') {
            const tx = toBlockchainTx(msg);
            await logToBlockchain(tx);
        } else {
            console.log(`[Skip] Transaction declined: ${RESPONSE_CODES[responseCode] || responseCode}`);
        }

        // Send acknowledgment if this is a request (0200)
        if (msg.mti === '0200') {
            const response = buildResponse(msg.mti, {
                3: f[3],
                7: formatDateTime(new Date()),
                11: f[11],
                37: f[37],
                38: f[38] || '000000',
                39: '00',
                41: f[41],
                42: f[42],
            });
            socket.write(response);
            console.log(`[Sent] MTI=0210 Response=00 (Acknowledged)`);
        }
        return;
    }

    // Handle Reversals (0400, 0420)
    if (['0400', '0420'].includes(msg.mti)) {
        console.log(`[Reversal] RRN=${f[37]}`);
        const tx = toBlockchainTx(msg);
        tx.txType = 'REVERSAL';
        await logToBlockchain(tx);

        // Send response
        const response = buildResponse(msg.mti, {
            7: formatDateTime(new Date()),
            11: f[11],
            37: f[37],
            39: '00',
        });
        socket.write(response);
        console.log(`[Sent] MTI=${parseInt(msg.mti) + 10} Response=00`);
        return;
    }

    console.log(`[Unknown] MTI=${msg.mti} - No handler`);
}

// ============= TCP SERVER =============

const connections = new Set();

/**
 * Read message length from header
 * Postilion can use either:
 * - 2-byte binary (big-endian) - most common
 * - 4-byte ASCII - some configurations
 */
function readMessageLength(buffer) {
    if (USE_ASCII_LENGTH_HEADER) {
        // 4-byte ASCII length header
        return parseInt(buffer.toString('ascii', 0, 4)) || 0;
    } else {
        // 2-byte binary big-endian (default for Postilion)
        return buffer.readUInt16BE(0);
    }
}

const server = net.createServer((socket) => {
    const clientInfo = `${socket.remoteAddress}:${socket.remotePort}`;
    console.log(`\n[Connect] Postilion connected from ${clientInfo}`);
    connections.add(socket);

    // Set socket options for reliability
    socket.setKeepAlive(true, 30000);  // Keep-alive every 30 seconds
    socket.setNoDelay(true);            // Disable Nagle algorithm

    let buffer = Buffer.alloc(0);

    socket.on('data', async (data) => {
        buffer = Buffer.concat([buffer, data]);

        if (LOG_RAW_DATA) {
            console.log(`[Raw] Received ${data.length} bytes: ${data.toString('hex').substring(0, 100)}...`);
        }

        // Process complete messages
        while (buffer.length >= HEADER_LENGTH) {
            const msgLen = readMessageLength(buffer);

            if (msgLen <= 0 || msgLen > 9999) {
                console.error(`[Error] Invalid message length: ${msgLen}, raw header: ${buffer.slice(0, HEADER_LENGTH).toString('hex')}`);
                // Try to recover by looking for MTI pattern
                const mtiIndex = buffer.toString('ascii').search(/0[1248][0-9]{2}/);
                if (mtiIndex > 0) {
                    console.log(`[Recovery] Found MTI at offset ${mtiIndex}, skipping bad data`);
                    buffer = buffer.slice(mtiIndex);
                    continue;
                }
                buffer = Buffer.alloc(0);
                break;
            }

            if (buffer.length >= msgLen + HEADER_LENGTH) {
                const message = buffer.slice(HEADER_LENGTH, msgLen + HEADER_LENGTH);
                buffer = buffer.slice(msgLen + HEADER_LENGTH);

                try {
                    await handleMessage(socket, message);
                } catch (err) {
                    console.error(`[Error] Processing message: ${err.message}`);
                }
            } else {
                // Wait for more data
                if (LOG_RAW_DATA) {
                    console.log(`[Wait] Need ${msgLen + HEADER_LENGTH - buffer.length} more bytes`);
                }
                break;
            }
        }
    });

    socket.on('close', () => {
        console.log(`[Disconnect] ${clientInfo}`);
        connections.delete(socket);
    });

    socket.on('error', (err) => {
        console.error(`[Socket Error] ${clientInfo}: ${err.message}`);
        connections.delete(socket);
    });
});

server.on('error', (err) => {
    console.error(`[Server Error] ${err.message}`);
});

// ============= START SERVER =============

console.log('╔════════════════════════════════════════════════════════════════╗');
console.log('║         REZO PostBridge Server - ISO 8583 Listener           ║');
console.log('║         Hyperledger Fabric Settlement System                 ║');
console.log('╠════════════════════════════════════════════════════════════════╣');
console.log(`║  Listen IP:      172.26.40.36                                 ║`);
console.log(`║  Listen Port:    ${LISTEN_PORT.toString().padEnd(43)}║`);
console.log(`║  Blockchain API: ${REZO_API_URL.padEnd(43)}║`);
console.log(`║  Length Header:  ${(USE_ASCII_LENGTH_HEADER ? '4-byte ASCII' : '2-byte Binary').padEnd(43)}║`);
console.log('╠════════════════════════════════════════════════════════════════╣');
console.log('║  POSTILION CONFIGURATION:                                     ║');
console.log('║    Remote Host IP:    172.26.40.36                            ║');
console.log(`║    Remote Host Port:  ${LISTEN_PORT.toString().padEnd(41)}║`);
console.log('║    Protocol:          TCP/IP                                  ║');
console.log('║    Header:            2-byte binary length (big-endian)       ║');
console.log('╚════════════════════════════════════════════════════════════════╝');
console.log('');
console.log('Waiting for Postilion to connect...\n');

server.listen(LISTEN_PORT, LISTEN_HOST, () => {
    console.log(`[Server] Listening on ${LISTEN_HOST}:${LISTEN_PORT}`);
});

// Graceful shutdown
process.on('SIGINT', () => {
    console.log('\n[Server] Shutting down...');
    connections.forEach(socket => socket.destroy());
    server.close();
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('\n[Server] Received SIGTERM, shutting down...');
    connections.forEach(socket => socket.destroy());
    server.close();
    process.exit(0);
});

// Log connection statistics periodically
setInterval(() => {
    if (connections.size > 0) {
        console.log(`[Stats] Active connections: ${connections.size}`);
    }
}, 60000);  // Every 60 seconds

// Handle uncaught errors
process.on('uncaughtException', (err) => {
    console.error('[Fatal] Uncaught exception:', err.message);
    console.error(err.stack);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('[Fatal] Unhandled rejection:', reason);
});
