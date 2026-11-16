# API Gateway with Dynamic Quota Limiter & Document Search

**Academic title:** Token-Tiered Ingress Rate Limiting Gateway and Lexical Document Retrieval Pipeline
**Stack:** MongoDB, Express.js 4, Angular 18 (standalone), Node.js. Runs entirely on localhost: no API keys, no cloud services, no Redis.

## Project structure

```
api-gateway-quota-limiter/
├── README.md
├── PROJECT_REPORT.md               ← full academic documentation report
├── backend/
│   ├── package.json                ← express, mongoose, cors
│   └── server.js                   ← entire gateway in one file
└── frontend/
    ├── package.json
    ├── angular.json
    ├── tsconfig.json
    ├── tsconfig.app.json
    └── src/
        ├── index.html              ← Tailwind CDN + Inter / JetBrains Mono
        ├── main.ts                 ← bootstrapApplication(AppComponent)
        ├── styles.css              ← palette, keyframes (shake, flash, shimmer…)
        └── app/
            ├── app.component.ts    ← standalone component, RxJS polling every 2 s
            └── app.component.html  ← 3-tab animated UI
```

## Prerequisites

| Tool    | Version                    | Check            |
|---------|----------------------------|------------------|
| Node.js | 18.19+, 20.11+ or 22.x     | `node -v`        |
| npm     | 9+                         | `npm -v`         |
| MongoDB | 6.x or 7.x on port 27017   | `mongosh --eval "db.runCommand({ping:1})"` |

No MongoDB installed? Either install MongoDB Community Server, or run it in Docker:

```bash
docker run -d --name gateway-mongo -p 27017:27017 mongo:7
```

## Run instructions

Open **three terminals** in the project root.

**Terminal 1: MongoDB** (skip if it already runs as a service or in Docker)

```bash
# macOS (Homebrew)
brew services start mongodb-community
# Linux (systemd)
sudo systemctl start mongod
# Windows (run as Administrator)
net start MongoDB
```

**Terminal 2: backend gateway (port 3000)**

```bash
cd backend
npm install
npm start
```

Expected output:

```
[mongo] connected to mongodb://localhost:27017/api_gateway_demo
[seed] inserted 4 knowledge documents
[seed] 2 API keys ready: Free Tier Key (5/min), Enterprise Pro Key (15/min)
[gateway] listening on http://localhost:3000
```

**Terminal 3: Angular frontend (port 4200)**

```bash
cd frontend
npm install
npm start
```

The browser opens at **http://localhost:4200**.

## Demo script (for the viva)

1. **Console tab:** keep *Free Tier Key* selected, click the example chip *refresh token rotation*. Watch Step 1 → 2 → 3 light up with server-measured timings and highlighted chunks.
2. **Radar tab:** click *Reset Quota*, then *Simulate Traffic Spike (8 Rapid Requests)*. The first 5 return HTTP 200, requests 6–8 return HTTP 429; the crimson banner shakes and counts down Retry-After.
3. Switch to *Enterprise Pro Key* and fire the spike again: all 8 succeed (limit 15).
4. Watch the timeline: green dots drift left and drop out after 60 s; the gauge drains and the gateway pill returns to *online*.
5. **Vault tab:** add a document (e.g. "API Key Rotation Runbook"), then search for its words in the Console tab.

## Testing the API with curl

```bash
# Search (protected)
curl -i -X POST http://localhost:3000/api/search \
  -H "x-api-key: free_tier_demo_key_5rpm" -H "Content-Type: application/json" \
  -d '{"query":"refresh token rotation"}'

# Quota snapshot
curl http://localhost:3000/api/quota/free_tier_demo_key_5rpm

# Reset one key / all keys
curl -X POST http://localhost:3000/api/reset-quota -H "Content-Type: application/json" -d '{"key":"free_tier_demo_key_5rpm"}'
curl -X POST http://localhost:3000/api/reset-quota -H "Content-Type: application/json" -d '{}'

# Documents
curl http://localhost:3000/api/documents
curl -X POST http://localhost:3000/api/documents -H "Content-Type: application/json" \
  -d '{"title":"API Key Rotation Runbook","docType":"PROTOCOL","tags":"keys, rotation","content":"Gateway API keys are rotated every thirty days with a seven day grace window."}'
```

Burst test (Bash): `for i in $(seq 1 8); do curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:3000/api/search -H "x-api-key: free_tier_demo_key_5rpm" -H "Content-Type: application/json" -d '{"query":"backup"}'; done` prints five `200` lines then three `429` lines.

## Configuration

| Variable    | Default                                        | Purpose                 |
|-------------|------------------------------------------------|-------------------------|
| `PORT`      | `3000`                                         | Backend HTTP port       |
| `MONGO_URI` | `mongodb://localhost:27017/api_gateway_demo`   | MongoDB connection      |

If you change `PORT`, also change `API_BASE` at the top of `frontend/src/app/app.component.ts`.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `[mongo] could not connect` and the backend exits | MongoDB is not running on 27017. Start it (see Terminal 1). |
| Header pill shows *Gateway offline* | Backend not running on port 3000. |
| Page is unstyled | The Tailwind CDN script could not load; check internet access for `cdn.tailwindcss.com` (needed by the browser only). |
| `EADDRINUSE :3000` | Another process uses port 3000; run with `PORT=3001 npm start` and update `API_BASE`. |
| Want a clean database | `mongosh api_gateway_demo --eval "db.dropDatabase()"` then restart the backend to re-seed. |

## Notes

* The limiter state lives in the Node process, so restarting the backend clears all windows (documents and keys persist in MongoDB).
* `HttpClientModule` is imported in the standalone component as the brief specifies. Angular 18 marks it deprecated in favour of `provideHttpClient()`; it still works with no runtime warning.
* The two API keys are deliberately exposed by `GET /api/keys` for the demo. A production gateway never lists secrets.
