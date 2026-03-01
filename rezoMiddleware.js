/**
 * REZO Middleware - Inline ISO 8583 Proxy
 * Sits between Acquirer/Postilion and Issuer
 *
 * Flow:
 *   Postilion ────▶ REZO ────▶ Issuer
 *   Postilion ◀──── REZO ◀──── Issuer
 *                    │
 *                    └──▶ Blockchain
 *
 * Configuration:
 *   - REZO listens on port 5000 (Postilion connects here)
 *   - REZO connects to Issuer on configured IP:Port
 *   - Records BOTH request and response to blockchain
 */

const net = require('net');
const axios = require('axios');

// ============= CONFIGURATION =============
// Postilion connects to REZO on this port
const LISTEN_PORT = process.env.LISTEN_PORT || 5000;
const LISTEN_HOST = process.env.LISTEN_HOST || '0.0.0.0';

// REZO connects to Issuer on this address
const ISSUER_HOST = process.env.ISSUER_HOST || '172.26.42.206';  // Issuer IP
const ISSUER_PORT = process.env.ISSUER_PORT || 4534;              // Issuer Port

// Blockchain API
const REZO_API_URL = process.env.REZO_API_URL || 'http://localhost:3000';

// Settings
const DEBUG = process.env.DEBUG === 'true' || process.env.DEBUG === '1';
const LOG_RAW_DATA = process.env.LOG_RAW === 'true' || process.env.LOG_RAW === '1';
const ISSUER_CONNECT_TIMEOUT = 10000;  // 10 seconds
const ISSUER_RECONNECT_INTERVAL = 5000;

// Debug logger
function debug(category, message, data = null) {
    if (!DEBUG) return;
    const timestamp = new Date().toISOString();
    console.log(`[DEBUG ${timestamp}] [${category}] ${message}`);
    if (data) {
        if (Buffer.isBuffer(data)) {
            console.log(`  HEX: ${data.toString('hex')}`);
            console.log(`  LEN: ${data.length} bytes`);
        } else if (typeof data === 'object') {
            console.log(`  DATA: ${JSON.stringify(data, null, 2)}`);
        } else {
            console.log(`  DATA: ${data}`);
        }
    }
}

// ============= ISO 8583 PARSING =============

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
    32:  ['llvar', 11, 2],     // Acquiring Institution ID
    33:  ['llvar', 11, 2],     // Forwarding Institution ID
    35:  ['llvar', 37, 2],     // Track 2 Data
    37:  ['fixed', 12, 0],     // RRN
    38:  ['fixed', 6, 0],      // Auth Code
    39:  ['fixed', 2, 0],      // Response Code
    41:  ['fixed', 8, 0],      // Terminal ID
    42:  ['fixed', 15, 0],     // Merchant ID
    43:  ['fixed', 40, 0],     // Merchant Name
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
    '09': 'PURCHASE',
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

function parseMessage(buffer) {
    const message = { fields: {}, raw: buffer };
    let offset = 0;

    try {
        message.mti = buffer.toString('ascii', offset, offset + 4);
        offset += 4;

        const primaryBitmap = buffer.slice(offset, offset + 8);
        offset += 8;

        let fullBitmap = primaryBitmap;
        if (primaryBitmap[0] & 0x80) {
            const secondaryBitmap = buffer.slice(offset, offset + 8);
            offset += 8;
            fullBitmap = Buffer.concat([primaryBitmap, secondaryBitmap]);
        }

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
        console.error('[Parse Error]', err.message);
    }

    return message;
}

function isBitSet(bitmap, bitNum) {
    if (bitNum < 1 || bitNum > bitmap.length * 8) return false;
    const byteIndex = Math.floor((bitNum - 1) / 8);
    const bitIndex = 7 - ((bitNum - 1) % 8);
    return (bitmap[byteIndex] & (1 << bitIndex)) !== 0;
}

function maskPAN(pan) {
    if (!pan || pan.length < 13) return pan;
    return pan.substring(0, 6) + '****' + pan.substring(pan.length - 4);
}

// ============= TRANSACTION TRACKING =============

// Store pending transactions awaiting response
const pendingTransactions = new Map();

function getTransactionKey(msg) {
    const f = msg.fields;
    // Use STAN + Transmission DateTime + Amount as unique key
    // (RRN can be parsed differently in response due to bitmap variations)
    const key = `${f[11]}-${f[7]}-${f[4]}`;
    debug('KEY', `Transaction key: ${key}`);
    return key;
}

// ============= BLOCKCHAIN LOGGING =============

async function logToBlockchain(request, response) {
    const reqFields = request.fields;
    const resFields = response.fields;

    const processingCode = (reqFields[3] || '000000').substring(0, 2);
    const txType = PROCESSING_CODES[processingCode] || 'PURCHASE';
    const amount = parseInt(reqFields[4] || '0') / 100;
    const responseCode = resFields[39] || 'XX';

    // Determine acquirer and issuer
    let acquirerCode = reqFields[32] || '';
    if (!acquirerCode || acquirerCode === '0') acquirerCode = 'ISW';

    let issuerCode = reqFields[100] || '';
    if (!issuerCode) {
        const pan = reqFields[2] || '';
        issuerCode = getBankFromBIN(pan.substring(0, 6)) || 'UNKNOWN';
    }

    const tx = {
        rrn: reqFields[37] || '',
        stan: reqFields[11] || '',
        maskedPan: maskPAN(reqFields[2]),
        acquirerCode: acquirerCode.trim(),
        issuerCode: issuerCode.trim(),
        terminalId: reqFields[41] || '',
        merchantId: reqFields[42] || '',
        merchantName: reqFields[43] || '',
        amount: amount,
        currency: 'NGN',
        txType: txType,
        authCode: resFields[38] || '',
        responseCode: responseCode,
        metadata: JSON.stringify({
            requestMTI: request.mti,
            responseMTI: response.mti,
            processingCode: reqFields[3],
            issuerConfirmed: true,
            responseDescription: RESPONSE_CODES[responseCode] || 'Unknown',
        }),
    };

    try {
        const result = await axios.post(`${REZO_API_URL}/transactions`, tx);
        console.log(`[Blockchain] ✓ RECORDED`);
        console.log(`             RRN: ${tx.rrn}`);
        console.log(`             Type: ${tx.txType}`);
        console.log(`             Amount: ₦${tx.amount.toLocaleString()}`);
        console.log(`             Flow: ${tx.acquirerCode} → ${tx.issuerCode}`);
        console.log(`             Response: ${responseCode} (${RESPONSE_CODES[responseCode] || 'Unknown'})`);
        console.log(`             Issuer Confirmed: ✓`);
        return result.data;
    } catch (err) {
        console.error(`[Blockchain] ✗ Error: ${err.message}`);
    }
}

function getBankFromBIN(bin) {
    const binRanges = {
        '539983': '058', '539941': '058',  // GTBank
        '544937': '044', '544927': '044',  // Access Bank
        '531083': '057', '531995': '057',  // Zenith Bank
        '519911': '033', '519940': '033',  // UBA
        '530988': '011', '539117': '011',  // First Bank
        '530220': '070',                    // Fidelity Bank
        '506105': '214', '506106': '214',  // FCMB
        '506107': '232',                    // Sterling
    };
    return binRanges[bin] || null;
}

// ============= PROXY SESSION =============

class ProxySession {
    constructor(acquirerSocket, sessionId) {
        this.sessionId = sessionId;
        this.acquirerSocket = acquirerSocket;
        this.issuerSocket = null;
        this.acquirerBuffer = Buffer.alloc(0);
        this.issuerBuffer = Buffer.alloc(0);
        this.isConnectedToIssuer = false;
        this.pendingMessages = [];

        this.connectToIssuer();
        this.setupAcquirerHandlers();
    }

    log(msg) {
        console.log(`[Session ${this.sessionId}] ${msg}`);
    }

    connectToIssuer() {
        this.log(`Connecting to Issuer ${ISSUER_HOST}:${ISSUER_PORT}...`);
        debug('ISSUER', `Initiating connection to ${ISSUER_HOST}:${ISSUER_PORT}`);

        this.issuerSocket = new net.Socket();
        this.issuerSocket.setTimeout(ISSUER_CONNECT_TIMEOUT);

        this.issuerSocket.connect(ISSUER_PORT, ISSUER_HOST, () => {
            this.log(`Connected to Issuer ✓`);
            debug('ISSUER', `Connection established successfully`);
            debug('ISSUER', `Local address: ${this.issuerSocket.localAddress}:${this.issuerSocket.localPort}`);
            this.isConnectedToIssuer = true;
            this.issuerSocket.setTimeout(0);

            // Send any pending messages
            while (this.pendingMessages.length > 0) {
                const msg = this.pendingMessages.shift();
                debug('ISSUER', `Sending queued message`, msg);
                this.forwardToIssuer(msg);
            }
        });

        this.issuerSocket.on('data', (data) => {
            debug('ISSUER', `Received data from Issuer`, data);
            this.handleIssuerData(data);
        });

        this.issuerSocket.on('error', (err) => {
            this.log(`Issuer connection error: ${err.message}`);
            debug('ISSUER', `Connection error: ${err.code} - ${err.message}`);
            debug('ISSUER', `Error details`, { code: err.code, errno: err.errno, syscall: err.syscall });
        });

        this.issuerSocket.on('close', (hadError) => {
            this.log(`Issuer connection closed`);
            debug('ISSUER', `Connection closed, hadError: ${hadError}`);
            this.isConnectedToIssuer = false;
        });

        this.issuerSocket.on('timeout', () => {
            this.log(`Issuer connection timeout`);
            debug('ISSUER', `Connection timeout after ${ISSUER_CONNECT_TIMEOUT}ms`);
            this.issuerSocket.destroy();
        });
    }

    setupAcquirerHandlers() {
        this.acquirerSocket.on('data', (data) => {
            debug('ACQUIRER', `Received data from Acquirer/Postilion`, data);
            this.handleAcquirerData(data);
        });

        this.acquirerSocket.on('close', (hadError) => {
            this.log(`Acquirer disconnected`);
            debug('ACQUIRER', `Connection closed, hadError: ${hadError}`);
            if (this.issuerSocket) this.issuerSocket.destroy();
        });

        this.acquirerSocket.on('error', (err) => {
            this.log(`Acquirer error: ${err.message}`);
            debug('ACQUIRER', `Error: ${err.code} - ${err.message}`);
        });
    }

    handleAcquirerData(data) {
        this.acquirerBuffer = Buffer.concat([this.acquirerBuffer, data]);
        debug('BUFFER', `Acquirer buffer size: ${this.acquirerBuffer.length} bytes`);

        while (this.acquirerBuffer.length >= 2) {
            const msgLen = this.acquirerBuffer.readUInt16BE(0);
            debug('PARSE', `Message length header: ${msgLen}`);

            if (msgLen <= 0 || msgLen > 9999) {
                debug('PARSE', `Invalid message length: ${msgLen}, clearing buffer`);
                this.acquirerBuffer = Buffer.alloc(0);
                break;
            }

            if (this.acquirerBuffer.length >= msgLen + 2) {
                const fullMessage = this.acquirerBuffer.slice(0, msgLen + 2);
                const messageBody = this.acquirerBuffer.slice(2, msgLen + 2);
                this.acquirerBuffer = this.acquirerBuffer.slice(msgLen + 2);

                debug('PARSE', `Extracted message: ${msgLen} bytes body`, messageBody);
                this.processAcquirerMessage(fullMessage, messageBody);
            } else {
                debug('PARSE', `Waiting for more data. Have ${this.acquirerBuffer.length}, need ${msgLen + 2}`);
                break;
            }
        }
    }

    processAcquirerMessage(fullMessage, messageBody) {
        const msg = parseMessage(messageBody);
        const f = msg.fields;

        debug('MSG', `Parsed message from Acquirer`, { mti: msg.mti, fields: f });

        console.log('═'.repeat(70));
        console.log(`[ACQUIRER → REZO] MTI=${msg.mti} RRN=${f[37] || 'N/A'}`);

        if (['0200', '0100', '0400', '0220'].includes(msg.mti)) {
            console.log(`[Request] ${PROCESSING_CODES[(f[3] || '').substring(0, 2)] || 'UNKNOWN'}`);
            console.log(`  PAN:      ${maskPAN(f[2])}`);
            console.log(`  Amount:   ₦${(parseInt(f[4] || '0') / 100).toLocaleString()}`);
            console.log(`  Terminal: ${f[41] || 'N/A'}`);
            console.log(`  Merchant: ${(f[43] || 'N/A').trim()}`);

            // Store request for matching with response
            const key = getTransactionKey(msg);
            pendingTransactions.set(key, { request: msg, timestamp: Date.now() });
        }

        // Forward to Issuer
        if (this.isConnectedToIssuer) {
            this.forwardToIssuer(fullMessage);
        } else {
            this.pendingMessages.push(fullMessage);
            this.log(`Queued message (waiting for issuer connection)`);
        }
    }

    forwardToIssuer(data) {
        if (this.issuerSocket && !this.issuerSocket.destroyed) {
            debug('FORWARD', `Sending to Issuer`, data);
            this.issuerSocket.write(data);
            console.log(`[REZO → ISSUER] Forwarded ${data.length} bytes`);
        } else {
            debug('FORWARD', `Cannot forward to Issuer - socket not available`);
            console.log(`[REZO → ISSUER] ERROR: Issuer socket not connected`);
        }
    }

    handleIssuerData(data) {
        this.issuerBuffer = Buffer.concat([this.issuerBuffer, data]);

        while (this.issuerBuffer.length >= 2) {
            const msgLen = this.issuerBuffer.readUInt16BE(0);

            if (msgLen <= 0 || msgLen > 9999) {
                this.issuerBuffer = Buffer.alloc(0);
                break;
            }

            if (this.issuerBuffer.length >= msgLen + 2) {
                const fullMessage = this.issuerBuffer.slice(0, msgLen + 2);
                const messageBody = this.issuerBuffer.slice(2, msgLen + 2);
                this.issuerBuffer = this.issuerBuffer.slice(msgLen + 2);

                this.processIssuerMessage(fullMessage, messageBody);
            } else {
                break;
            }
        }
    }

    async processIssuerMessage(fullMessage, messageBody) {
        const msg = parseMessage(messageBody);
        const f = msg.fields;

        debug('MSG', `Parsed message from Issuer`, { mti: msg.mti, fields: f });

        console.log(`[ISSUER → REZO] MTI=${msg.mti} RRN=${f[37] || 'N/A'} Response=${f[39] || 'N/A'}`);

        // Check if this is a response to a pending request
        if (['0210', '0110', '0410', '0230'].includes(msg.mti)) {
            const key = getTransactionKey(msg);
            const pending = pendingTransactions.get(key);

            if (pending) {
                console.log(`[Response] ${RESPONSE_CODES[f[39]] || 'Unknown'}`);

                // *** THIS IS THE KEY MOMENT ***
                // Both request and response are now available
                // Log to blockchain with issuer's confirmation
                await logToBlockchain(pending.request, msg);

                pendingTransactions.delete(key);
            } else {
                console.log(`[Response] No matching request found`);
            }
        }

        // Forward response back to Acquirer
        this.forwardToAcquirer(fullMessage);
    }

    forwardToAcquirer(data) {
        if (this.acquirerSocket && !this.acquirerSocket.destroyed) {
            debug('FORWARD', `Sending to Acquirer/Postilion`, data);
            this.acquirerSocket.write(data);
            console.log(`[REZO → ACQUIRER] Forwarded ${data.length} bytes`);
        } else {
            debug('FORWARD', `Cannot forward to Acquirer - socket not available`);
            console.log(`[REZO → ACQUIRER] ERROR: Acquirer socket not connected`);
        }
    }

    destroy() {
        if (this.acquirerSocket) this.acquirerSocket.destroy();
        if (this.issuerSocket) this.issuerSocket.destroy();
    }
}

// ============= MAIN SERVER =============

let sessionCounter = 0;
const sessions = new Map();

const server = net.createServer((socket) => {
    const sessionId = ++sessionCounter;
    const clientInfo = `${socket.remoteAddress}:${socket.remotePort}`;

    console.log(`\n[New Connection] Session ${sessionId} from ${clientInfo}`);

    const session = new ProxySession(socket, sessionId);
    sessions.set(sessionId, session);

    socket.on('close', () => {
        sessions.delete(sessionId);
    });
});

// ============= STARTUP =============

console.log('╔════════════════════════════════════════════════════════════════════╗');
console.log('║              REZO MIDDLEWARE - ISO 8583 INLINE PROXY               ║');
console.log('║                                                                    ║');
console.log('║   Sits between Acquirer/Postilion and Issuer                       ║');
console.log('║   Records transactions when Issuer confirms                        ║');
console.log('╠════════════════════════════════════════════════════════════════════╣');
console.log('║                                                                    ║');
console.log('║   ACQUIRER/POSTILION  ────▶  REZO  ────▶  ISSUER                  ║');
console.log('║   ACQUIRER/POSTILION  ◀────  REZO  ◀────  ISSUER                  ║');
console.log('║                               │                                    ║');
console.log('║                               └──▶ Blockchain                      ║');
console.log('║                                                                    ║');
console.log('╠════════════════════════════════════════════════════════════════════╣');
console.log(`║  Listening for Acquirer on:    ${LISTEN_HOST}:${LISTEN_PORT}`.padEnd(69) + '║');
console.log(`║  Forwarding to Issuer at:      ${ISSUER_HOST}:${ISSUER_PORT}`.padEnd(69) + '║');
console.log(`║  Blockchain API:               ${REZO_API_URL}`.padEnd(69) + '║');
console.log(`║  Debug Mode:                   ${DEBUG ? 'ON' : 'OFF'}`.padEnd(69) + '║');
console.log('╠════════════════════════════════════════════════════════════════════╣');
console.log('║  POSTILION CONFIG:                                                 ║');
console.log('║    Connect to:  172.26.40.36:5000 (REZO)                          ║');
console.log('║    Instead of:  Issuer directly                                    ║');
console.log('╚════════════════════════════════════════════════════════════════════╝');
console.log('');

server.listen(LISTEN_PORT, LISTEN_HOST, () => {
    console.log(`[Server] Listening on ${LISTEN_HOST}:${LISTEN_PORT}`);
    console.log(`[Server] Will forward to Issuer at ${ISSUER_HOST}:${ISSUER_PORT}`);
    console.log('');
    console.log('Waiting for connections...\n');
});

// Cleanup
process.on('SIGINT', () => {
    console.log('\n[Server] Shutting down...');
    sessions.forEach(session => session.destroy());
    server.close();
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('\n[Server] Shutting down...');
    sessions.forEach(session => session.destroy());
    server.close();
    process.exit(0);
});

// Clean up old pending transactions periodically
setInterval(() => {
    const now = Date.now();
    const timeout = 120000; // 2 minutes

    for (const [key, value] of pendingTransactions) {
        if (now - value.timestamp > timeout) {
            pendingTransactions.delete(key);
        }
    }
}, 60000);
