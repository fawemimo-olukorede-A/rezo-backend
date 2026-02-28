/**
 * ISO 8583 Message Listener
 * Receives ISO 8583 messages from the payment switch and logs them to blockchain
 *
 * ISO 8583 is the standard for card transaction messaging
 * Common Message Types:
 * - 0100: Authorization Request
 * - 0110: Authorization Response
 * - 0200: Financial Request (Purchase/Withdrawal)
 * - 0210: Financial Response
 * - 0400: Reversal Request
 * - 0410: Reversal Response
 */

const net = require('net');
const axios = require('axios');

// Configuration
const ISO_PORT = process.env.ISO_PORT || 5000;
const REZO_API_URL = process.env.REZO_API_URL || 'http://localhost:3000';

// ISO 8583 Field Definitions (simplified for Nigerian card transactions)
const ISO_FIELDS = {
    0: { name: 'MTI', length: 4, type: 'fixed' },           // Message Type Indicator
    2: { name: 'PAN', length: 19, type: 'llvar' },          // Primary Account Number
    3: { name: 'ProcessingCode', length: 6, type: 'fixed' },// Processing Code
    4: { name: 'Amount', length: 12, type: 'fixed' },       // Transaction Amount
    7: { name: 'TransmissionDateTime', length: 10, type: 'fixed' },
    11: { name: 'STAN', length: 6, type: 'fixed' },         // System Trace Audit Number
    12: { name: 'LocalTime', length: 6, type: 'fixed' },
    13: { name: 'LocalDate', length: 4, type: 'fixed' },
    14: { name: 'ExpiryDate', length: 4, type: 'fixed' },
    18: { name: 'MerchantType', length: 4, type: 'fixed' },
    22: { name: 'POSEntryMode', length: 3, type: 'fixed' },
    23: { name: 'CardSequenceNumber', length: 3, type: 'fixed' },
    25: { name: 'POSConditionCode', length: 2, type: 'fixed' },
    26: { name: 'POSPinCaptureCode', length: 2, type: 'fixed' },
    32: { name: 'AcquiringInstitution', length: 11, type: 'llvar' }, // Acquirer Code
    33: { name: 'ForwardingInstitution', length: 11, type: 'llvar' },
    35: { name: 'Track2Data', length: 37, type: 'llvar' },
    37: { name: 'RRN', length: 12, type: 'fixed' },         // Retrieval Reference Number
    38: { name: 'AuthCode', length: 6, type: 'fixed' },     // Authorization Code
    39: { name: 'ResponseCode', length: 2, type: 'fixed' }, // Response Code (00 = approved)
    41: { name: 'TerminalID', length: 8, type: 'fixed' },   // Card Acceptor Terminal ID
    42: { name: 'MerchantID', length: 15, type: 'fixed' },  // Card Acceptor ID
    43: { name: 'MerchantName', length: 40, type: 'fixed' },// Card Acceptor Name/Location
    49: { name: 'CurrencyCode', length: 3, type: 'fixed' }, // Currency Code (566 = NGN)
    52: { name: 'PINData', length: 16, type: 'fixed' },
    100: { name: 'ReceivingInstitution', length: 11, type: 'llvar' }, // Issuer Code
    102: { name: 'AccountID1', length: 28, type: 'llvar' },
    103: { name: 'AccountID2', length: 28, type: 'llvar' },
    123: { name: 'POSDataCode', length: 15, type: 'lllvar' },
    128: { name: 'MAC', length: 16, type: 'fixed' },
};

// Processing codes for transaction types
const PROCESSING_CODES = {
    '00': 'PURCHASE',        // Purchase/Sale
    '01': 'WITHDRAWAL',      // Cash Withdrawal
    '20': 'REFUND',          // Refund
    '31': 'BALANCE_INQUIRY', // Balance Inquiry
    '40': 'TRANSFER',        // Transfer
};

// Currency codes
const CURRENCY_CODES = {
    '566': 'NGN',  // Nigerian Naira
    '840': 'USD',  // US Dollar
    '978': 'EUR',  // Euro
};

/**
 * Parse ISO 8583 message (simplified parser)
 */
function parseISO8583(buffer) {
    const message = {};
    let offset = 0;

    try {
        // Read MTI (first 4 bytes)
        message.mti = buffer.toString('ascii', offset, offset + 4);
        offset += 4;

        // Read bitmap (16 bytes for primary + secondary bitmap in hex)
        const bitmapHex = buffer.toString('hex', offset, offset + 16);
        offset += 16;

        // Parse bitmap to determine which fields are present
        const bitmap = BigInt('0x' + bitmapHex);

        // Parse fields based on bitmap
        for (let i = 2; i <= 128; i++) {
            const bitPosition = BigInt(1) << BigInt(128 - i);
            if (bitmap & bitPosition) {
                const fieldDef = ISO_FIELDS[i];
                if (fieldDef) {
                    let fieldLength = fieldDef.length;
                    let fieldValue;

                    // Handle variable length fields
                    if (fieldDef.type === 'llvar') {
                        fieldLength = parseInt(buffer.toString('ascii', offset, offset + 2));
                        offset += 2;
                    } else if (fieldDef.type === 'lllvar') {
                        fieldLength = parseInt(buffer.toString('ascii', offset, offset + 3));
                        offset += 3;
                    }

                    fieldValue = buffer.toString('ascii', offset, offset + fieldLength).trim();
                    offset += fieldLength;

                    message[fieldDef.name] = fieldValue;
                }
            }
        }
    } catch (err) {
        console.error('ISO 8583 Parse Error:', err.message);
    }

    return message;
}

/**
 * Mask PAN for security (show first 6 and last 4 digits)
 */
function maskPAN(pan) {
    if (!pan || pan.length < 13) return pan;
    const first6 = pan.substring(0, 6);
    const last4 = pan.substring(pan.length - 4);
    const masked = '*'.repeat(pan.length - 10);
    return `${first6}${masked}${last4}`;
}

/**
 * Convert ISO 8583 message to blockchain transaction format
 */
function convertToBlockchainTx(isoMessage) {
    // Determine transaction type from processing code
    const processingCode = isoMessage.ProcessingCode?.substring(0, 2) || '00';
    const txType = PROCESSING_CODES[processingCode] || 'PURCHASE';

    // Convert currency code
    const currencyCode = isoMessage.CurrencyCode || '566';
    const currency = CURRENCY_CODES[currencyCode] || 'NGN';

    // Parse amount (ISO 8583 amounts are typically in minor units)
    const amountInMinor = parseInt(isoMessage.Amount || '0');
    const amount = amountInMinor / 100; // Convert kobo to Naira

    return {
        rrn: isoMessage.RRN || '',
        stan: isoMessage.STAN || '',
        maskedPan: maskPAN(isoMessage.PAN),
        acquirerCode: isoMessage.AcquiringInstitution || '',
        issuerCode: isoMessage.ReceivingInstitution || '',
        terminalId: isoMessage.TerminalID || '',
        merchantId: isoMessage.MerchantID || '',
        merchantName: isoMessage.MerchantName || '',
        amount: amount,
        currency: currency,
        txType: txType,
        authCode: isoMessage.AuthCode || '',
        responseCode: isoMessage.ResponseCode || '',
        metadata: JSON.stringify({
            mti: isoMessage.mti,
            processingCode: isoMessage.ProcessingCode,
            posEntryMode: isoMessage.POSEntryMode,
            merchantType: isoMessage.MerchantType,
        }),
    };
}

/**
 * Send transaction to blockchain via Rezo API
 */
async function logToBlockchain(tx) {
    try {
        const response = await axios.post(`${REZO_API_URL}/transactions`, tx);
        console.log(`[Blockchain] Transaction logged: RRN=${tx.rrn}, Amount=${tx.amount} ${tx.currency}`);
        return response.data;
    } catch (err) {
        console.error(`[Blockchain] Error logging transaction: ${err.message}`);
        throw err;
    }
}

/**
 * Handle incoming ISO 8583 message
 */
async function handleMessage(buffer) {
    console.log(`[ISO8583] Received ${buffer.length} bytes`);

    // Parse ISO 8583 message
    const isoMessage = parseISO8583(buffer);
    console.log(`[ISO8583] MTI: ${isoMessage.mti}, RRN: ${isoMessage.RRN}`);

    // Only process financial transactions (0200, 0210, 0220)
    // 0200 = Financial Request, 0210 = Financial Response
    const validMTIs = ['0200', '0210', '0220', '0100', '0110'];
    if (!validMTIs.includes(isoMessage.mti)) {
        console.log(`[ISO8583] Skipping non-financial message: MTI=${isoMessage.mti}`);
        return null;
    }

    // Only log approved transactions (response code 00)
    if (isoMessage.ResponseCode && isoMessage.ResponseCode !== '00') {
        console.log(`[ISO8583] Skipping declined transaction: ResponseCode=${isoMessage.ResponseCode}`);
        return null;
    }

    // Convert to blockchain format and log
    const blockchainTx = convertToBlockchainTx(isoMessage);
    return await logToBlockchain(blockchainTx);
}

/**
 * Start ISO 8583 TCP Server
 */
function startServer() {
    const server = net.createServer((socket) => {
        console.log(`[ISO8583] Client connected: ${socket.remoteAddress}:${socket.remotePort}`);

        let buffer = Buffer.alloc(0);

        socket.on('data', async (data) => {
            // Accumulate data (messages may be fragmented)
            buffer = Buffer.concat([buffer, data]);

            // Check if we have a complete message (first 4 bytes = length header)
            while (buffer.length >= 4) {
                const messageLength = parseInt(buffer.toString('ascii', 0, 4));

                if (buffer.length >= messageLength + 4) {
                    const message = buffer.slice(4, messageLength + 4);
                    buffer = buffer.slice(messageLength + 4);

                    try {
                        await handleMessage(message);
                    } catch (err) {
                        console.error(`[ISO8583] Error handling message: ${err.message}`);
                    }
                } else {
                    // Wait for more data
                    break;
                }
            }
        });

        socket.on('close', () => {
            console.log(`[ISO8583] Client disconnected: ${socket.remoteAddress}`);
        });

        socket.on('error', (err) => {
            console.error(`[ISO8583] Socket error: ${err.message}`);
        });
    });

    server.listen(ISO_PORT, () => {
        console.log(`[ISO8583] Listener started on port ${ISO_PORT}`);
        console.log(`[ISO8583] Forwarding to Rezo API: ${REZO_API_URL}`);
    });

    return server;
}

// Export for use as module or run standalone
module.exports = { startServer, parseISO8583, convertToBlockchainTx };

// Run standalone if executed directly
if (require.main === module) {
    console.log('=== ISO 8583 to Blockchain Bridge ===');
    console.log('Listening for ISO 8583 messages from payment switch...');
    startServer();
}
