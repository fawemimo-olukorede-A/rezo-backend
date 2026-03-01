/**
 * REZO Issuer Bridge Server
 * Receives transaction confirmations from Issuer Banks
 *
 * When an issuer authorizes a transaction, they send confirmation to REZO
 * This creates bilateral confirmation (both acquirer and issuer agree)
 *
 * Configure Issuer to connect to:
 *   IP:   172.26.40.36
 *   PORT: 5001
 */

const net = require('net');
const axios = require('axios');

// ============= CONFIGURATION =============
const LISTEN_PORT = process.env.ISSUER_PORT || 5001;
const LISTEN_HOST = process.env.LISTEN_HOST || '0.0.0.0';
const REZO_API_URL = process.env.REZO_API_URL || 'http://localhost:3000';

// ============= ISO 8583 FIELD DEFINITIONS =============

const FIELD_SPECS = {
    2:   ['llvar', 19, 2],     // PAN
    3:   ['fixed', 6, 0],      // Processing Code
    4:   ['fixed', 12, 0],     // Amount
    7:   ['fixed', 10, 0],     // Transmission Date & Time
    11:  ['fixed', 6, 0],      // STAN
    12:  ['fixed', 6, 0],      // Local Time
    13:  ['fixed', 4, 0],      // Local Date
    32:  ['llvar', 11, 2],     // Acquiring Institution
    37:  ['fixed', 12, 0],     // RRN
    38:  ['fixed', 6, 0],      // Auth Code
    39:  ['fixed', 2, 0],      // Response Code
    41:  ['fixed', 8, 0],      // Terminal ID
    42:  ['fixed', 15, 0],     // Merchant ID
    43:  ['fixed', 40, 0],     // Merchant Name
    49:  ['fixed', 3, 0],      // Currency Code
    70:  ['fixed', 3, 0],      // Network Management Code
    100: ['llvar', 11, 2],     // Receiving Institution (Issuer)
    102: ['llvar', 28, 2],     // Account ID
};

const PROCESSING_CODES = {
    '00': 'PURCHASE',
    '01': 'WITHDRAWAL',
    '20': 'REFUND',
    '31': 'BALANCE_INQUIRY',
    '40': 'TRANSFER',
};

const RESPONSE_CODES = {
    '00': 'Approved',
    '05': 'Do not honor',
    '14': 'Invalid card',
    '51': 'Insufficient funds',
    '54': 'Expired card',
    '55': 'Incorrect PIN',
    '91': 'Issuer unavailable',
};

// ============= MESSAGE PARSING =============

function parseMessage(buffer) {
    const message = { fields: {} };
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

            let fieldValue = buffer.toString('ascii', offset, offset + fieldLength);
            offset += fieldLength;
            message.fields[fieldNum] = fieldValue.trim();
        }
    } catch (err) {
        console.error('[Issuer Parser] Error:', err.message);
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
    const responseMTI = (parseInt(mti) + 10).toString().padStart(4, '0');
    parts.push(Buffer.from(responseMTI, 'ascii'));

    const bitmap = Buffer.alloc(8);
    const fieldNums = Object.keys(fields).map(Number);

    for (const field of fieldNums) {
        if (field < 1 || field > 64) continue;
        const byteIndex = Math.floor((field - 1) / 8);
        const bitIndex = 7 - ((field - 1) % 8);
        bitmap[byteIndex] |= (1 << bitIndex);
    }
    parts.push(bitmap);

    for (const fieldNum of fieldNums.sort((a, b) => a - b)) {
        const value = fields[fieldNum];
        const spec = FIELD_SPECS[fieldNum];
        if (!spec) continue;

        const [type, maxLen, lenDigits] = spec;
        let fieldData;

        if (type === 'fixed') {
            fieldData = Buffer.from(value.toString().padEnd(maxLen, ' ').substring(0, maxLen), 'ascii');
        } else if (type === 'llvar') {
            const dataStr = value.toString().substring(0, maxLen);
            const lenStr = dataStr.length.toString().padStart(2, '0');
            fieldData = Buffer.from(lenStr + dataStr, 'ascii');
        }

        if (fieldData) parts.push(fieldData);
    }

    const messageBody = Buffer.concat(parts);
    const lengthHeader = Buffer.alloc(2);
    lengthHeader.writeUInt16BE(messageBody.length, 0);

    return Buffer.concat([lengthHeader, messageBody]);
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

// ============= ISSUER CONFIRMATION =============

/**
 * Confirm transaction on blockchain (issuer's confirmation)
 */
async function confirmTransaction(msg) {
    const f = msg.fields;
    const rrn = f[37] || '';
    const responseCode = f[39] || '';
    const issuerCode = f[100] || f[32] || 'UNKNOWN';

    try {
        // Call the issuer confirmation endpoint
        const response = await axios.post(`${REZO_API_URL}/transactions/issuer-confirm`, {
            rrn: rrn,
            issuerCode: issuerCode,
            responseCode: responseCode,
            authCode: f[38] || '',
            confirmedAt: new Date().toISOString(),
        });

        console.log(`[Blockchain] ✓ Issuer Confirmed: RRN=${rrn}, Issuer=${issuerCode}, Response=${responseCode}`);
        return response.data;
    } catch (err) {
        // If the specific endpoint doesn't exist, try recording as a new transaction
        if (err.response?.status === 404) {
            console.log(`[Blockchain] Issuer confirmation endpoint not found, recording as transaction`);
            return await recordTransaction(msg);
        }
        console.error(`[Blockchain] ✗ Error: ${err.message}`);
    }
}

/**
 * Record transaction if confirmation endpoint doesn't exist
 */
async function recordTransaction(msg) {
    const f = msg.fields;
    const processingCode = (f[3] || '000000').substring(0, 2);
    const txType = PROCESSING_CODES[processingCode] || 'PURCHASE';
    const amount = parseInt(f[4] || '0') / 100;

    const tx = {
        rrn: f[37] || '',
        stan: f[11] || '',
        maskedPan: maskPAN(f[2]),
        acquirerCode: f[32] || 'UNKNOWN',
        issuerCode: f[100] || 'UNKNOWN',
        terminalId: f[41] || '',
        merchantId: f[42] || '',
        merchantName: f[43] || '',
        amount: amount,
        currency: 'NGN',
        txType: txType,
        authCode: f[38] || '',
        responseCode: f[39] || '',
        metadata: JSON.stringify({
            source: 'ISSUER',
            mti: msg.mti,
            confirmedByIssuer: true,
        }),
    };

    try {
        const response = await axios.post(`${REZO_API_URL}/transactions`, tx);
        console.log(`[Blockchain] ✓ Recorded from Issuer: RRN=${tx.rrn}, ${tx.txType}, ₦${tx.amount.toLocaleString()}`);
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
    console.log(`[Issuer] Received: MTI=${msg.mti} RRN=${f[37] || 'N/A'}`);

    // Handle Network Management (0800)
    if (msg.mti === '0800') {
        const networkCode = f[70];
        console.log(`[Issuer Network] Code=${networkCode}`);

        const response = buildResponse(msg.mti, {
            7: formatDateTime(new Date()),
            11: f[11],
            39: '00',
            70: f[70],
        });
        socket.write(response);
        console.log(`[Issuer] Sent: MTI=0810 Response=00`);
        return;
    }

    // Handle Authorization Response from Issuer (0110, 0210)
    // This is when the issuer is confirming they processed the transaction
    if (['0110', '0210', '0410'].includes(msg.mti)) {
        const responseCode = f[39] || '00';
        const issuerCode = f[100] || 'UNKNOWN';

        console.log(`[Issuer Confirmation]`);
        console.log(`  RRN:       ${f[37] || 'N/A'}`);
        console.log(`  Issuer:    ${issuerCode}`);
        console.log(`  Response:  ${responseCode} (${RESPONSE_CODES[responseCode] || 'Unknown'})`);
        console.log(`  Amount:    ₦${(parseInt(f[4] || '0') / 100).toLocaleString()}`);

        // Record issuer's confirmation
        await confirmTransaction(msg);

        // Acknowledge
        const response = buildResponse(msg.mti, {
            7: formatDateTime(new Date()),
            11: f[11],
            37: f[37],
            39: '00',
        });
        socket.write(response);
        console.log(`[Issuer] Sent: Acknowledgment`);
        return;
    }

    // Handle original authorization messages if issuer sends them
    if (['0100', '0200', '0400'].includes(msg.mti)) {
        console.log(`[Issuer] Processing original message MTI=${msg.mti}`);
        await recordTransaction(msg);

        const response = buildResponse(msg.mti, {
            7: formatDateTime(new Date()),
            11: f[11],
            37: f[37],
            39: '00',
        });
        socket.write(response);
        return;
    }

    console.log(`[Issuer] Unknown MTI: ${msg.mti}`);
}

// ============= TCP SERVER =============

const connections = new Set();

const server = net.createServer((socket) => {
    const clientInfo = `${socket.remoteAddress}:${socket.remotePort}`;
    console.log(`\n[Issuer Connect] ${clientInfo}`);
    connections.add(socket);

    socket.setKeepAlive(true, 30000);
    socket.setNoDelay(true);

    let buffer = Buffer.alloc(0);

    socket.on('data', async (data) => {
        buffer = Buffer.concat([buffer, data]);

        while (buffer.length >= 2) {
            const msgLen = buffer.readUInt16BE(0);

            if (msgLen <= 0 || msgLen > 9999) {
                console.error(`[Issuer] Invalid length: ${msgLen}`);
                buffer = Buffer.alloc(0);
                break;
            }

            if (buffer.length >= msgLen + 2) {
                const message = buffer.slice(2, msgLen + 2);
                buffer = buffer.slice(msgLen + 2);

                try {
                    await handleMessage(socket, message);
                } catch (err) {
                    console.error(`[Issuer] Error: ${err.message}`);
                }
            } else {
                break;
            }
        }
    });

    socket.on('close', () => {
        console.log(`[Issuer Disconnect] ${clientInfo}`);
        connections.delete(socket);
    });

    socket.on('error', (err) => {
        console.error(`[Issuer Error] ${clientInfo}: ${err.message}`);
        connections.delete(socket);
    });
});

// ============= START SERVER =============

console.log('╔════════════════════════════════════════════════════════════════╗');
console.log('║         REZO Issuer Bridge - ISO 8583 Listener                 ║');
console.log('║         Receives Transaction Confirmations from Issuers        ║');
console.log('╠════════════════════════════════════════════════════════════════╣');
console.log(`║  Listen IP:      172.26.40.36                                  ║`);
console.log(`║  Listen Port:    ${LISTEN_PORT.toString().padEnd(43)}║`);
console.log(`║  Blockchain API: ${REZO_API_URL.padEnd(43)}║`);
console.log('╠════════════════════════════════════════════════════════════════╣');
console.log('║  ISSUER BANK CONFIGURATION:                                    ║');
console.log('║    Remote Host IP:    172.26.40.36                             ║');
console.log(`║    Remote Host Port:  ${LISTEN_PORT.toString().padEnd(41)}║`);
console.log('║    Protocol:          TCP/IP                                   ║');
console.log('║    Message:           0210 (Auth Response) after authorization ║');
console.log('╚════════════════════════════════════════════════════════════════╝');
console.log('');
console.log('Waiting for Issuer banks to connect...\n');

server.listen(LISTEN_PORT, LISTEN_HOST, () => {
    console.log(`[Issuer Server] Listening on ${LISTEN_HOST}:${LISTEN_PORT}`);
});

process.on('SIGINT', () => {
    console.log('\n[Issuer Server] Shutting down...');
    connections.forEach(socket => socket.destroy());
    server.close();
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('\n[Issuer Server] Shutting down...');
    connections.forEach(socket => socket.destroy());
    server.close();
    process.exit(0);
});
