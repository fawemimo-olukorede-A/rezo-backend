# Rezo Backend

Backend API for Hyperledger Fabric Settlement Chaincode.

## Setup

### 1. Install Dependencies

```bash
npm install
```

### 2. Configure Certificates

Copy the required certificates from your Fabric network:

```bash
# Peer TLS Certificate (for secure connection to peer)
cp ~/fabric-network/organizations/peerOrganizations/switchorg.example.com/peers/peer0.switchorg.example.com/tls/ca.crt ./peer-tls-cert.pem

# Admin User Certificate
cp ~/fabric-network/organizations/peerOrganizations/switchorg.example.com/users/Admin@switchorg.example.com/msp/signcerts/cert.pem ./wallet/cert.pem

# Admin User Private Key (find the actual key file name)
cp ~/fabric-network/organizations/peerOrganizations/switchorg.example.com/users/Admin@switchorg.example.com/msp/keystore/*_sk ./wallet/key.pem
```

### 3. Environment Variables (Optional)

Copy `.env.example` to `.env` and adjust values:

```bash
cp .env.example .env
```

### 4. Start the Server

```bash
# Production
npm start

# Development (with auto-reload)
npm run dev
```

## API Endpoints

### Health Check

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |

### Transactions

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/transactions` | Get all transactions (switch only) |
| GET | `/transactions/:txId` | Get transaction by ID |
| GET | `/transactions/window?startTime=&endTime=` | Get transactions in time window |
| POST | `/transactions` | Submit new transaction |

### Bank Operations

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/bank/transactions` | Get transactions for calling org |
| GET | `/bank/balance/:orgMSPID` | Get balance for an org |

### Analytics

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/participants` | Get unique participants |
| GET | `/analytics` | Get analytics summary |

### Settlement

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/settlement/finalize` | Finalize settlement window |
| POST | `/settlement/clear` | Clear specific transactions |

### Ledger

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/ledger/init` | Initialize ledger |

## Example Requests

### Submit Transaction

```bash
curl -X POST http://localhost:3000/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "payerOrg": "alphamorganorgMSP",
    "payeeOrg": "bytaorgMSP",
    "amount": 50000,
    "currency": "NGN",
    "metadata": "Payment for services"
  }'
```

### Get All Transactions

```bash
curl http://localhost:3000/transactions
```

### Get Balance

```bash
curl http://localhost:3000/bank/balance/alphamorganorgMSP
```

### Finalize Settlement

```bash
curl -X POST http://localhost:3000/settlement/finalize \
  -H "Content-Type: application/json" \
  -d '{
    "windowStart": "2026-02-01T00:00:00Z",
    "windowEnd": "2026-02-28T23:59:59Z"
  }'
```

### Clear Transactions

```bash
curl -X POST http://localhost:3000/settlement/clear \
  -H "Content-Type: application/json" \
  -d '{
    "txIds": ["tx-id-1", "tx-id-2"]
  }'
```

## Project Structure

```
rezo-backend/
├── app.js              # Express server and routes
├── fabricGateway.js    # Fabric Gateway connection helper
├── package.json        # Dependencies
├── .env.example        # Environment variables template
├── peer-tls-cert.pem   # Peer TLS certificate (you provide)
├── wallet/
│   ├── cert.pem        # Admin certificate (you provide)
│   └── key.pem         # Admin private key (you provide)
└── README.md
```
