const express = require('express');
const cors = require('cors');
const { execSync } = require('child_process');
const { createGateway, closeGateway } = require('./fabricGateway');

const app = express();
app.use(cors());
app.use(express.json());

// Channel and chaincode configuration
const CHANNEL_NAME = process.env.CHANNEL_NAME || 'mychannel';
const CHAINCODE_NAME = process.env.CHAINCODE_NAME || 'settlementv3';

/**
 * Helper to safely parse JSON response from chaincode
 */
function safeParseJSON(data, defaultValue = []) {
    if (!data || data.length === 0) {
        return defaultValue;
    }

    // Convert Uint8Array to Buffer if needed, then to string
    let str;
    if (data instanceof Uint8Array || ArrayBuffer.isView(data)) {
        str = Buffer.from(data).toString('utf8');
    } else if (Buffer.isBuffer(data)) {
        str = data.toString('utf8');
    } else {
        str = String(data);
    }

    str = str.trim();
    if (!str || str === 'null' || str === 'undefined') {
        return defaultValue;
    }

    try {
        return JSON.parse(str);
    } catch (e) {
        console.error('JSON parse error. Raw response:', str.substring(0, 200));
        throw new Error(`Invalid JSON response: ${str.substring(0, 100)}`);
    }
}

/**
 * Helper to execute a chaincode query (evaluate)
 */
async function executeQuery(functionName, ...args) {
    const { gateway, client } = await createGateway();
    try {
        const network = gateway.getNetwork(CHANNEL_NAME);
        const contract = network.getContract(CHAINCODE_NAME);
        const result = await contract.evaluateTransaction(functionName, ...args);
        return safeParseJSON(result, []);
    } finally {
        await closeGateway(gateway, client);
    }
}

/**
 * Helper to execute a chaincode transaction (submit) using peer CLI
 * This ensures proper endorsement from multiple orgs
 */
async function executeTransaction(functionName, ...args) {
    // Build the peer CLI command with multiple endorsing peers
    const FABRIC_PATH = process.env.FABRIC_PATH || '/home/compadmin/fabric-network';

    // Escape args for JSON
    const escapedArgs = args.map(arg => arg.replace(/"/g, '\\"'));
    const argsJson = `["${escapedArgs.join('","')}"]`;
    const invokeJson = JSON.stringify({ function: functionName, Args: JSON.parse(argsJson) });

    // Path to peer binary
    const PEER_BIN = process.env.PEER_BIN || '/home/compadmin/fabric/fabric-samples/bin/peer';

    const cmd = `
        export FABRIC_CFG_PATH="${FABRIC_PATH}/"
        export CORE_PEER_TLS_ENABLED=true
        export CORE_PEER_LOCALMSPID="switchorgMSP"
        export CORE_PEER_TLS_ROOTCERT_FILE="${FABRIC_PATH}/organizations/peerOrganizations/switchorg.example.com/peers/peer0.switchorg.example.com/tls/ca.crt"
        export CORE_PEER_MSPCONFIGPATH="${FABRIC_PATH}/organizations/peerOrganizations/switchorg.example.com/users/Admin@switchorg.example.com/msp"
        export CORE_PEER_ADDRESS=localhost:7051

        ${PEER_BIN} chaincode invoke \\
            -o localhost:7050 \\
            --ordererTLSHostnameOverride orderer.example.com \\
            --tls \\
            --cafile "${FABRIC_PATH}/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/tls/ca.crt" \\
            -C ${CHANNEL_NAME} \\
            -n ${CHAINCODE_NAME} \\
            --peerAddresses localhost:7051 \\
            --tlsRootCertFiles "${FABRIC_PATH}/organizations/peerOrganizations/switchorg.example.com/peers/peer0.switchorg.example.com/tls/ca.crt" \\
            --peerAddresses peer0.bytaorg.rezo.com:7051 \\
            --tlsRootCertFiles "${FABRIC_PATH}/organizations/peerOrganizations/bytaorg.rezo.com/peers/peer0.bytaorg.rezo.com/tls/ca.crt" \\
            -c '${invokeJson.replace(/'/g, "'\\''")}' 2>&1
    `.trim();

    try {
        const result = execSync(cmd, {
            shell: '/bin/bash',
            encoding: 'utf8',
            timeout: 60000
        });
        console.log('Peer CLI result:', result);
        return { success: true };
    } catch (error) {
        console.error('Peer CLI error:', error.message);
        if (error.stdout) console.error('stdout:', error.stdout);
        if (error.stderr) console.error('stderr:', error.stderr);
        throw new Error(error.stderr || error.stdout || error.message);
    }
}

// =====================================================
// ROUTES
// =====================================================

/**
 * Health check endpoint
 */
app.get('/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

/**
 * GET /transactions
 * Returns all transactions (switchorgMSP only)
 */
app.get('/transactions', async (req, res) => {
    try {
        const transactions = await executeQuery('GetAllTransactions');
        res.json(transactions);
    } catch (err) {
        console.error('Error GET /transactions:', err.message);
        res.status(500).json({ error: err.message });
    }
});

/**
 * GET /transactions/:txId
 * Get a specific transaction by ID
 */
app.get('/transactions/:txId', async (req, res) => {
    try {
        const { txId } = req.params;
        const transaction = await executeQuery('GetTransaction', txId);
        res.json(transaction);
    } catch (err) {
        console.error('Error GET /transactions/:txId:', err.message);
        res.status(500).json({ error: err.message });
    }
});

/**
 * GET /transactions/window
 * Get transactions within a time window
 * Query params: startTime, endTime (RFC3339 format)
 */
app.get('/transactions/window', async (req, res) => {
    try {
        const { startTime, endTime } = req.query;
        if (!startTime || !endTime) {
            return res.status(400).json({ error: 'startTime and endTime are required (RFC3339 format)' });
        }
        const transactions = await executeQuery('GetTransactionsInWindow', startTime, endTime);
        res.json(transactions);
    } catch (err) {
        console.error('Error GET /transactions/window:', err.message);
        res.status(500).json({ error: err.message });
    }
});

/**
 * GET /bank/transactions
 * Get transactions for the calling organization
 */
app.get('/bank/transactions', async (req, res) => {
    try {
        const transactions = await executeQuery('GetBankTransactions');
        res.json(transactions);
    } catch (err) {
        console.error('Error GET /bank/transactions:', err.message);
        res.status(500).json({ error: err.message });
    }
});

/**
 * GET /bank/balance/:orgMSPID
 * Get balance for a specific organization
 */
app.get('/bank/balance/:orgMSPID', async (req, res) => {
    try {
        const { orgMSPID } = req.params;
        const balance = await executeQuery('GetBankBalance', orgMSPID);
        res.json(balance);
    } catch (err) {
        console.error('Error GET /bank/balance:', err.message);
        res.status(500).json({ error: err.message });
    }
});

/**
 * GET /participants
 * Extract unique participants from all transactions (supports both old and new field names)
 */
app.get('/participants', async (req, res) => {
    try {
        const transactions = await executeQuery('GetAllTransactions');
        const uniqueParticipants = [...new Set(
            transactions.flatMap((tx) => [
                tx.acquirerCode || tx.payerOrg,
                tx.issuerCode || tx.payeeOrg
            ])
        )].filter(Boolean);
        res.json(uniqueParticipants);
    } catch (err) {
        console.error('Error GET /participants:', err.message);
        res.status(500).json({ error: err.message });
    }
});

/**
 * GET /analytics
 * Get analytics summary of all transactions (supports card transactions)
 */
app.get('/analytics', async (req, res) => {
    try {
        const transactions = await executeQuery('GetAllTransactions');

        const totalTransactions = transactions.length;
        const totalVolume = transactions.reduce((sum, tx) => sum + (tx.amount || 0), 0);
        const settledCount = transactions.filter(tx => tx.status === 'SETTLED').length;
        const pendingCount = transactions.filter(tx => tx.status === 'PENDING').length;
        const disputedCount = transactions.filter(tx => tx.status === 'DISPUTED').length;

        // Group by currency
        const volumeByCurrency = transactions.reduce((acc, tx) => {
            const currency = tx.currency || 'NGN';
            acc[currency] = (acc[currency] || 0) + (tx.amount || 0);
            return acc;
        }, {});

        // Group by transaction type (card transactions)
        const volumeByTxType = transactions.reduce((acc, tx) => {
            const txType = tx.txType || 'TRANSFER';
            acc[txType] = acc[txType] || { count: 0, volume: 0 };
            acc[txType].count++;
            acc[txType].volume += tx.amount || 0;
            return acc;
        }, {});

        // Group by participant (supports both old and new field names)
        const volumeByParticipant = {};
        transactions.forEach(tx => {
            // Acquirer (or payerOrg for legacy)
            const acquirer = tx.acquirerCode || tx.payerOrg;
            if (acquirer) {
                volumeByParticipant[acquirer] = volumeByParticipant[acquirer] || { sent: 0, received: 0, asAcquirer: 0, asIssuer: 0 };
                volumeByParticipant[acquirer].sent += tx.amount || 0;
                volumeByParticipant[acquirer].asAcquirer += tx.amount || 0;
            }
            // Issuer (or payeeOrg for legacy)
            const issuer = tx.issuerCode || tx.payeeOrg;
            if (issuer) {
                volumeByParticipant[issuer] = volumeByParticipant[issuer] || { sent: 0, received: 0, asAcquirer: 0, asIssuer: 0 };
                volumeByParticipant[issuer].received += tx.amount || 0;
                volumeByParticipant[issuer].asIssuer += tx.amount || 0;
            }
        });

        // Net positions between acquirer-issuer pairs
        const netPositions = {};
        transactions.filter(tx => tx.status === 'PENDING').forEach(tx => {
            const acquirer = tx.acquirerCode || tx.payerOrg;
            const issuer = tx.issuerCode || tx.payeeOrg;
            if (acquirer && issuer) {
                const key = `${acquirer}~${issuer}`;
                netPositions[key] = netPositions[key] || { acquirer, issuer, count: 0, amount: 0 };
                netPositions[key].count++;
                netPositions[key].amount += tx.amount || 0;
            }
        });

        res.json({
            totalTransactions,
            totalVolume,
            settledCount,
            pendingCount,
            disputedCount,
            volumeByCurrency,
            volumeByTxType,
            volumeByParticipant,
            netPositions: Object.values(netPositions),
            currency: 'NGN'
        });
    } catch (err) {
        console.error('Error GET /analytics:', err.message);
        res.status(500).json({ error: err.message });
    }
});

/**
 * POST /transactions
 * Submit a new card transaction from the switch
 * Supports both new card transaction format and legacy interbank format
 */
app.post('/transactions', async (req, res) => {
    try {
        const {
            // New card transaction fields
            rrn, stan, maskedPan,
            acquirerCode, issuerCode,
            terminalId, merchantId, merchantName,
            amount, currency, txType,
            authCode, responseCode,
            // Legacy fields (for backward compatibility)
            payerOrg, payeeOrg,
            metadata
        } = req.body;

        // Determine if this is a card transaction or legacy interbank transaction
        const isCardTransaction = acquirerCode || issuerCode || rrn;

        if (isCardTransaction) {
            // Card transaction validation
            if (!acquirerCode || !issuerCode || !amount) {
                return res.status(400).json({
                    error: 'acquirerCode, issuerCode, and amount are required for card transactions'
                });
            }

            if (typeof amount !== 'number' || amount <= 0) {
                return res.status(400).json({
                    error: 'amount must be a positive number'
                });
            }

            await executeTransaction(
                'SubmitCardTransaction',
                rrn || '',
                stan || '',
                maskedPan || '',
                acquirerCode,
                issuerCode,
                terminalId || '',
                merchantId || '',
                merchantName || '',
                amount.toString(),
                currency || 'NGN',
                txType || 'PURCHASE',
                authCode || '',
                responseCode || '00',
                metadata || ''
            );

            res.status(201).json({
                success: true,
                message: 'Card transaction submitted successfully',
                rrn: rrn || 'auto-generated'
            });
        } else {
            // Legacy interbank transaction (backward compatibility)
            if (!payerOrg || !payeeOrg || !amount) {
                return res.status(400).json({
                    error: 'payerOrg, payeeOrg, and amount are required'
                });
            }

            if (typeof amount !== 'number' || amount <= 0) {
                return res.status(400).json({
                    error: 'amount must be a positive number'
                });
            }

            await executeTransaction(
                'SubmitInterBankTransaction',
                payerOrg,
                payeeOrg,
                amount.toString(),
                currency || '',
                metadata || ''
            );

            res.status(201).json({ success: true, message: 'Transaction submitted successfully' });
        }
    } catch (err) {
        console.error('Error POST /transactions:', err.message);
        res.status(500).json({ error: err.message });
    }
});

/**
 * GET /transactions/rrn/:rrn
 * Get a transaction by RRN (Retrieval Reference Number)
 */
app.get('/transactions/rrn/:rrn', async (req, res) => {
    try {
        const { rrn } = req.params;
        const transaction = await executeQuery('GetTransactionByRRN', rrn);
        res.json(transaction);
    } catch (err) {
        console.error('Error GET /transactions/rrn/:rrn:', err.message);
        res.status(404).json({ error: err.message });
    }
});

/**
 * GET /transactions/acquirer/:code
 * Get all transactions for an acquirer
 */
app.get('/transactions/acquirer/:code', async (req, res) => {
    try {
        const { code } = req.params;
        const transactions = await executeQuery('GetTransactionsByAcquirer', code);
        res.json(transactions);
    } catch (err) {
        console.error('Error GET /transactions/acquirer/:code:', err.message);
        res.status(500).json({ error: err.message });
    }
});

/**
 * GET /transactions/issuer/:code
 * Get all transactions for an issuer
 */
app.get('/transactions/issuer/:code', async (req, res) => {
    try {
        const { code } = req.params;
        const transactions = await executeQuery('GetTransactionsByIssuer', code);
        res.json(transactions);
    } catch (err) {
        console.error('Error GET /transactions/issuer/:code:', err.message);
        res.status(500).json({ error: err.message });
    }
});

/**
 * GET /transactions/date/:date
 * Get all transactions for a specific date (YYYY-MM-DD)
 */
app.get('/transactions/date/:date', async (req, res) => {
    try {
        const { date } = req.params;
        const transactions = await executeQuery('GetTransactionsByDate', date);
        res.json(transactions);
    } catch (err) {
        console.error('Error GET /transactions/date/:date:', err.message);
        res.status(500).json({ error: err.message });
    }
});

/**
 * GET /settlement/netposition
 * Get net settlement position between acquirer and issuer
 * Query params: acquirerCode, issuerCode
 */
app.get('/settlement/netposition', async (req, res) => {
    try {
        const { acquirerCode, issuerCode } = req.query;
        if (!acquirerCode || !issuerCode) {
            return res.status(400).json({
                error: 'acquirerCode and issuerCode are required'
            });
        }
        const position = await executeQuery('GetNetPosition', acquirerCode, issuerCode);
        res.json(position);
    } catch (err) {
        console.error('Error GET /settlement/netposition:', err.message);
        res.status(500).json({ error: err.message });
    }
});

/**
 * POST /transactions/dispute
 * Raise a dispute on a transaction
 */
app.post('/transactions/dispute', async (req, res) => {
    try {
        const { txId, reason } = req.body;

        if (!txId || !reason) {
            return res.status(400).json({
                error: 'txId and reason are required'
            });
        }

        await executeTransaction('RaiseDispute', txId, reason);
        res.json({ success: true, message: 'Dispute raised successfully' });
    } catch (err) {
        console.error('Error POST /transactions/dispute:', err.message);
        res.status(500).json({ error: err.message });
    }
});

/**
 * POST /settlement/finalize
 * Finalize settlement for a time window (switchorgMSP only)
 */
app.post('/settlement/finalize', async (req, res) => {
    try {
        const { windowStart, windowEnd } = req.body;

        if (!windowStart || !windowEnd) {
            return res.status(400).json({
                error: 'windowStart and windowEnd are required (RFC3339 format)'
            });
        }

        const result = await executeTransaction('FinalizeSettlement', windowStart, windowEnd);
        res.json(result);
    } catch (err) {
        console.error('Error POST /settlement/finalize:', err.message);
        res.status(500).json({ error: err.message });
    }
});

/**
 * POST /settlement/clear
 * Clear specific transactions (switchorgMSP only)
 */
app.post('/settlement/clear', async (req, res) => {
    try {
        const { txIds } = req.body;

        if (!txIds || !Array.isArray(txIds)) {
            return res.status(400).json({
                error: 'txIds must be an array of transaction IDs'
            });
        }

        const result = await executeTransaction('ClearTransactions', JSON.stringify(txIds));
        res.json(result);
    } catch (err) {
        console.error('Error POST /settlement/clear:', err.message);
        res.status(500).json({ error: err.message });
    }
});

/**
 * POST /ledger/init
 * Initialize the ledger
 */
app.post('/ledger/init', async (req, res) => {
    try {
        await executeTransaction('InitLedger');
        res.json({ success: true, message: 'Ledger initialized' });
    } catch (err) {
        console.error('Error POST /ledger/init:', err.message);
        res.status(500).json({ error: err.message });
    }
});

app.get('/debug/raw', async (req, res) => {
    const { gateway, client } = await createGateway();
    try {
        const network = gateway.getNetwork(CHANNEL_NAME);
        const contract = network.getContract(CHAINCODE_NAME);
        const result = await contract.evaluateTransaction('GetAllTransactions');

        res.json({
            length: result.length,
            hex: result.toString('hex'),
            utf8: result.toString('utf8'),
            base64: result.toString('base64')
        });
    } catch (err) {
        res.status(500).json({ error: err.message, stack: err.stack });
    } finally {
        await closeGateway(gateway, client);
    }
});


// =====================================================
// AUTO-SETTLEMENT SCHEDULER
// =====================================================

const SETTLEMENT_INTERVAL = process.env.SETTLEMENT_INTERVAL || 50000; // 50 seconds default
let autoSettlementEnabled = process.env.AUTO_SETTLEMENT !== 'false'; // enabled by default
let settlementTimer = null;

/**
 * Run automatic settlement for all pending transactions
 */
async function runAutoSettlement() {
    if (!autoSettlementEnabled) return;

    try {
        // Create a time window from 24 hours ago to now
        const now = new Date();
        const windowStart = new Date(now.getTime() - 24 * 60 * 60 * 1000); // 24 hours ago
        const windowEnd = now;

        console.log(`[Auto-Settlement] Running at ${now.toISOString()}`);
        console.log(`[Auto-Settlement] Window: ${windowStart.toISOString()} to ${windowEnd.toISOString()}`);

        // First check if there are pending transactions
        const transactions = await executeQuery('GetAllTransactions');
        const pendingCount = transactions.filter(tx => tx.status === 'PENDING').length;

        if (pendingCount === 0) {
            console.log('[Auto-Settlement] No pending transactions to settle');
            return;
        }

        console.log(`[Auto-Settlement] Found ${pendingCount} pending transactions`);

        // Execute settlement
        await executeTransaction(
            'FinalizeSettlement',
            windowStart.toISOString(),
            windowEnd.toISOString()
        );

        console.log('[Auto-Settlement] Settlement completed successfully');
    } catch (err) {
        console.error('[Auto-Settlement] Error:', err.message);
    }
}

/**
 * Start the auto-settlement scheduler
 */
function startAutoSettlement() {
    if (settlementTimer) {
        clearInterval(settlementTimer);
    }

    console.log(`[Auto-Settlement] Starting scheduler (interval: ${SETTLEMENT_INTERVAL}ms)`);
    settlementTimer = setInterval(runAutoSettlement, SETTLEMENT_INTERVAL);

    // Run once immediately after a short delay (give time for startup)
    setTimeout(runAutoSettlement, 5000);
}

/**
 * Stop the auto-settlement scheduler
 */
function stopAutoSettlement() {
    if (settlementTimer) {
        clearInterval(settlementTimer);
        settlementTimer = null;
        console.log('[Auto-Settlement] Scheduler stopped');
    }
}

/**
 * GET /settlement/auto/status
 * Get auto-settlement status
 */
app.get('/settlement/auto/status', (req, res) => {
    res.json({
        enabled: autoSettlementEnabled,
        interval: SETTLEMENT_INTERVAL,
        running: settlementTimer !== null
    });
});

/**
 * POST /settlement/auto/start
 * Start auto-settlement
 */
app.post('/settlement/auto/start', (req, res) => {
    autoSettlementEnabled = true;
    startAutoSettlement();
    res.json({ success: true, message: 'Auto-settlement started', interval: SETTLEMENT_INTERVAL });
});

/**
 * POST /settlement/auto/stop
 * Stop auto-settlement
 */
app.post('/settlement/auto/stop', (req, res) => {
    autoSettlementEnabled = false;
    stopAutoSettlement();
    res.json({ success: true, message: 'Auto-settlement stopped' });
});

/**
 * POST /settlement/auto/run
 * Trigger immediate settlement
 */
app.post('/settlement/auto/run', async (req, res) => {
    try {
        await runAutoSettlement();
        res.json({ success: true, message: 'Settlement triggered' });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// =====================================================
// ERROR HANDLING
// =====================================================

// 404 handler
app.use((req, res) => {
    res.status(404).json({ error: 'Not found' });
});

// Global error handler
app.use((err, req, res, next) => {
    console.error('Unhandled error:', err);
    res.status(500).json({ error: 'Internal server error' });
});

// =====================================================
// SERVER START
// =====================================================

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Rezo Backend listening on port ${PORT}`);
    console.log(`Channel: ${CHANNEL_NAME}`);
    console.log(`Chaincode: ${CHAINCODE_NAME}`);

    // Start auto-settlement if enabled
    if (autoSettlementEnabled) {
        startAutoSettlement();
    }
});
