'use strict';

/* =============================================================================
 *  Token-Tiered Ingress Rate Limiting Gateway
 *  and Lexical Document Retrieval Pipeline
 *  ---------------------------------------------------------------------------
 *  Single-file backend: Node.js + Express + Mongoose (MongoDB on localhost).
 *  No external APIs, no Redis. The rate limiter is an in-process
 *  Sliding-Window Log; API keys and documents live in MongoDB.
 * ============================================================================= */

const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const { performance } = require('node:perf_hooks');

/* -----------------------------------------------------------------------------
 * 1. Configuration
 * ---------------------------------------------------------------------------*/
const PORT = Number(process.env.PORT) || 3000;
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/api_gateway_demo';

const DEFAULT_WINDOW_MS = 60 * 1000;   // 60-second sliding window
const KEY_CACHE_TTL_MS = 30 * 1000;    // re-read key metadata from Mongo every 30 s
const CHUNK_TARGET_WORDS = 45;         // target size of a retrievable chunk
const MAX_EVENTS_PER_KEY = 40;         // recent traffic events kept per key
const MAX_CANDIDATES = 500;            // upper bound on documents scored per query
const MAX_RESULTS = 5;
const MAX_CHUNKS_PER_RESULT = 3;
const MAX_QUERY_LENGTH = 200;

// BM25 parameters (chunk-level scoring)
const BM25_K1 = 1.2;
const BM25_B = 0.75;
const TITLE_BOOST = 1.5;
const TAG_BOOST = 1.0;

const DOC_TYPES = ['POLICY', 'PROTOCOL', 'SLA', 'GUIDELINE', 'NOTE'];

const DEFAULT_API_KEYS = [
  {
    key: 'free_tier_demo_key_5rpm',
    label: 'Free Tier Key',
    tier: 'FREE',
    limitPerWindow: 5,
    windowMs: DEFAULT_WINDOW_MS,
    active: true,
  },
  {
    key: 'enterprise_pro_demo_key_15rpm',
    label: 'Enterprise Pro Key',
    tier: 'ENTERPRISE_PRO',
    limitPerWindow: 15,
    windowMs: DEFAULT_WINDOW_MS,
    active: true,
  },
];

const SEED_DOCUMENTS = [
  {
    title: 'OAuth2 Security Architecture',
    docType: 'POLICY',
    tags: ['oauth2', 'security', 'tokens', 'identity'],
    content:
      'OAuth2 delegates authorization through short-lived access tokens issued by a central authorization server. ' +
      'Every public client must use the Authorization Code flow with PKCE; the implicit flow is prohibited because tokens leak through browser history and referrer headers. ' +
      'Access tokens expire after 15 minutes and are signed with RS256 keys that are rotated every 90 days through the JWKS endpoint. ' +
      'Refresh tokens are bound to the client, stored server-side and rotated on every use, so replaying a stolen refresh token revokes the entire token family. ' +
      'Resource servers validate the issuer, audience, expiry and scope claims on every request and never trust tokens forwarded from untrusted networks. ' +
      'Scopes follow least privilege: read and write permissions are separated, and administrative scopes require step-up multi-factor authentication. ' +
      'Token introspection results are cached for at most 60 seconds to limit revocation lag. ' +
      'Client secrets are never embedded in mobile or single page applications. ' +
      'All token endpoints sit behind the API gateway, which enforces per-key rate limits to blunt credential stuffing and brute force attacks against the authorization server.',
  },
  {
    title: 'Database Disaster Recovery Protocol',
    docType: 'PROTOCOL',
    tags: ['database', 'backup', 'recovery', 'rpo', 'rto'],
    content:
      'This protocol defines how the platform restores data after hardware failure, corruption or a regional outage. ' +
      'The recovery point objective is five minutes and the recovery time objective is thirty minutes for tier-one databases. ' +
      'MongoDB runs as a three-member replica set spread across separate availability zones, with an additional hidden delayed member lagging one hour behind the primary to protect against accidental deletes. ' +
      'Continuous oplog backups are shipped to immutable object storage every five minutes, and full snapshots are taken nightly and retained for thirty-five days. ' +
      'Backups are encrypted at rest with keys held in a separate account. ' +
      'Restore drills run every quarter: an engineer restores the latest snapshot into an isolated environment, replays the oplog to a chosen timestamp and verifies checksums against production. ' +
      'During an incident the on-call engineer declares severity, freezes schema migrations, promotes a healthy secondary if the primary is lost, and redirects application traffic through the gateway once write availability is confirmed. ' +
      'Every recovery ends with a blameless postmortem and an update to this runbook.',
  },
  {
    title: 'Microservice SLA Guarantees',
    docType: 'SLA',
    tags: ['sla', 'latency', 'availability', 'microservices'],
    content:
      'Each production microservice publishes a service level agreement that is measured at the API gateway rather than inside the service. ' +
      'Tier-one services guarantee 99.95 percent monthly availability, which allows roughly twenty-two minutes of downtime per month. ' +
      'Latency objectives are stated as percentiles: the p95 response time must stay below 250 milliseconds and the p99 below 800 milliseconds for synchronous calls. ' +
      'Error budgets are derived from these targets; when a service burns more than half of its monthly budget, feature releases pause until reliability work restores the margin. ' +
      'Callers must use timeouts, retries with exponential backoff and jitter, and circuit breakers so that one slow dependency cannot exhaust shared thread pools. ' +
      'Requests rejected with HTTP 429 because a client exceeded its quota do not count against the provider availability, since throttling is the contracted behaviour that protects every other tenant. ' +
      'Enterprise customers receive higher request quotas and priority support, while free tier consumers are served on a best-effort basis. ' +
      'Monthly SLA reports are generated automatically from gateway access logs.',
  },
  {
    title: 'Zero-Trust Perimeter Guidelines',
    docType: 'GUIDELINE',
    tags: ['zero-trust', 'network', 'mtls', 'identity'],
    content:
      'Zero trust assumes the network is already compromised, so no request is trusted because of where it comes from. ' +
      'Every call between services is authenticated with mutual TLS using short-lived workload certificates issued by an internal certificate authority. ' +
      'Authorization decisions are made per request by evaluating identity, device posture and context against central policy, never by IP allow lists alone. ' +
      'The API gateway is the single ingress point: it terminates TLS, validates API keys or tokens, enforces rate limits and strips untrusted headers before forwarding traffic. ' +
      'Network segmentation limits lateral movement, and administrative access requires just-in-time elevation that expires automatically. ' +
      'All access decisions are logged to an append-only audit trail, and anomalies such as sudden traffic spikes, impossible travel or repeated authorization failures raise alerts. ' +
      'Secrets live in a managed vault and are never stored in source code or container images. ' +
      'Teams review these guidelines every six months and verify compliance through automated configuration scanning.',
  },
];

/* -----------------------------------------------------------------------------
 * 2. Lexical utilities: tokenizer, light stemmer, chunker, highlighter
 * ---------------------------------------------------------------------------*/
const STOPWORDS = new Set([
  'a', 'an', 'and', 'are', 'as', 'at', 'be', 'been', 'but', 'by', 'can', 'do', 'does', 'for', 'from',
  'had', 'has', 'have', 'how', 'if', 'in', 'into', 'is', 'it', 'its', 'me', 'my', 'no', 'not', 'of',
  'on', 'or', 'our', 'so', 'such', 'than', 'that', 'the', 'their', 'them', 'then', 'there', 'these',
  'they', 'this', 'those', 'to', 'up', 'was', 'we', 'were', 'what', 'when', 'where', 'which', 'while',
  'who', 'why', 'will', 'with', 'you', 'your', 'about', 'after', 'all', 'also', 'any', 'because',
  'before', 'being', 'both', 'each', 'every', 'should', 'would', 'could', 'must', 'may', 'over',
  'only', 'other', 'per', 'via', 'once', 'one',
]);

/** Conservative suffix stripper so "tokens" matches "token" and "policies" matches "policy". */
function stem(word) {
  const w = word;
  if (w.length > 4 && w.endsWith('ies')) return `${w.slice(0, -3)}y`;
  if (w.length > 4 && /(ss|x|ch|sh)es$/.test(w)) return w.slice(0, -2);
  if (w.length > 3 && w.endsWith('s') && !/(ss|us|is)$/.test(w)) return w.slice(0, -1);
  return w;
}

/** Lower-case, strip diacritics/punctuation, drop stop-words and 1-char tokens, stem. */
function tokenize(text) {
  return String(text || '')
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .split(/[^a-z0-9]+/)
    .filter((t) => t.length >= 2 && !STOPWORDS.has(t))
    .map(stem);
}

function uniqueTokens(text) {
  return [...new Set(tokenize(text))];
}

function countWords(text) {
  const m = String(text || '').trim().match(/\S+/g);
  return m ? m.length : 0;
}

/** Sentence-aware chunker: groups whole sentences until ~CHUNK_TARGET_WORDS words. */
function chunkText(content) {
  const sentences = String(content || '')
    .replace(/\s+/g, ' ')
    .trim()
    .split(/(?<=[.!?])\s+/)
    .filter(Boolean);

  const chunks = [];
  let buffer = [];
  let bufferWords = 0;
  for (const sentence of sentences) {
    const words = countWords(sentence);
    if (bufferWords > 0 && bufferWords + words > CHUNK_TARGET_WORDS) {
      chunks.push(buffer.join(' '));
      buffer = [];
      bufferWords = 0;
    }
    buffer.push(sentence);
    bufferWords += words;
  }
  if (buffer.length) chunks.push(buffer.join(' '));
  return chunks.map((text, index) => ({ index, text, wordCount: countWords(text) }));
}

/** Split text into [{text, hit}] segments; a word is a hit if its stem is a query term. */
function highlight(text, termSet) {
  const parts = String(text || '').split(/([A-Za-z0-9]+)/);
  const segments = [];
  for (let i = 0; i < parts.length; i += 1) {
    const part = parts[i];
    if (!part) continue;
    const isWord = i % 2 === 1;
    const hit = isWord && termSet.has(stem(part.toLowerCase()));
    const last = segments[segments.length - 1];
    if (last && last.hit === hit && !hit) {
      last.text += part;
    } else {
      segments.push({ text: part, hit });
    }
  }
  return segments;
}

function normalizeTags(input) {
  const list = Array.isArray(input) ? input : String(input || '').split(',');
  return [...new Set(
    list
      .map((t) => String(t).trim().toLowerCase().replace(/\s+/g, '-'))
      .filter((t) => t.length > 0 && t.length <= 32),
  )].slice(0, 12);
}

function escapeRegex(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

const round = (n, d = 2) => Number(Number(n).toFixed(d));

/* -----------------------------------------------------------------------------
 * 3. Persistence layer: Mongoose schemas
 * ---------------------------------------------------------------------------*/
const apiKeySchema = new mongoose.Schema(
  {
    key: { type: String, required: true, unique: true, trim: true },
    label: { type: String, required: true, trim: true },
    tier: { type: String, enum: ['FREE', 'ENTERPRISE_PRO'], required: true },
    limitPerWindow: { type: Number, required: true, min: 1, max: 100000 },
    windowMs: { type: Number, default: DEFAULT_WINDOW_MS, min: 1000 },
    active: { type: Boolean, default: true },
  },
  { timestamps: true, collection: 'apiKeys' },
);

const chunkSchema = new mongoose.Schema(
  {
    index: { type: Number, required: true },
    text: { type: String, required: true },
    wordCount: { type: Number, required: true },
  },
  { _id: false },
);

const knowledgeDocumentSchema = new mongoose.Schema(
  {
    title: { type: String, required: true, trim: true, minlength: 3, maxlength: 160 },
    docType: { type: String, enum: DOC_TYPES, default: 'NOTE' },
    content: { type: String, required: true, minlength: 20, maxlength: 20000 },
    tags: { type: [String], default: [] },
    wordCount: { type: Number, default: 0 },
    chunks: { type: [chunkSchema], default: [] },
    tokens: { type: [String], default: [], index: true }, // multikey inverted index
  },
  { timestamps: true, collection: 'knowledgeDocuments' },
);

// Derive word count, chunks and the token index every time a document is validated.
knowledgeDocumentSchema.pre('validate', function deriveLexicalFields() {
  this.tags = normalizeTags(this.tags);
  this.wordCount = countWords(this.content);
  this.chunks = chunkText(this.content);
  this.tokens = uniqueTokens(`${this.title} ${this.tags.join(' ')} ${this.content}`);
});

const ApiKey = mongoose.model('ApiKey', apiKeySchema);
const KnowledgeDocument = mongoose.model('KnowledgeDocument', knowledgeDocumentSchema);

/* -----------------------------------------------------------------------------
 * 4. Sliding-Window Log rate-limit store (in-process, no Redis)
 * ---------------------------------------------------------------------------*/
class SlidingWindowLog {
  constructor() {
    this.logs = new Map();   // apiKey -> ascending array of accepted request timestamps (ms)
    this.events = new Map(); // apiKey -> ring buffer of recent {at, status}
  }

  /** Remove timestamps that fell out of the window (t <= now - windowMs). O(k) for k expired. */
  prune(key, windowMs, now) {
    const log = this.logs.get(key) || [];
    let expired = 0;
    while (expired < log.length && log[expired] <= now - windowMs) expired += 1;
    if (expired > 0) log.splice(0, expired);
    this.logs.set(key, log);
    return log;
  }

  /** Atomic check-and-record. Node's single thread makes this race-free within one process. */
  tryConsume(key, limit, windowMs, now = Date.now()) {
    const log = this.prune(key, windowMs, now);
    if (log.length >= limit) {
      return {
        allowed: false,
        limit,
        used: log.length,
        remaining: 0,
        retryAfterMs: log[0] + windowMs - now,
        resetMs: log[0] + windowMs - now,
      };
    }
    log.push(now);
    return {
      allowed: true,
      limit,
      used: log.length,
      remaining: limit - log.length,
      retryAfterMs: 0,
      resetMs: log[0] + windowMs - now,
    };
  }

  snapshot(key, limit, windowMs, now = Date.now()) {
    const log = this.prune(key, windowMs, now);
    const used = log.length;
    return {
      used,
      remaining: Math.max(0, limit - used),
      timestamps: [...log],
      resetInMs: used ? log[0] + windowMs - now : 0,
      retryAfterMs: used >= limit ? log[0] + windowMs - now : 0,
      events: [...(this.events.get(key) || [])],
    };
  }

  recordEvent(key, status, now = Date.now()) {
    const list = this.events.get(key) || [];
    list.push({ at: now, status });
    if (list.length > MAX_EVENTS_PER_KEY) list.splice(0, list.length - MAX_EVENTS_PER_KEY);
    this.events.set(key, list);
  }

  reset(key) {
    if (key) {
      this.logs.delete(key);
      this.events.delete(key);
    } else {
      this.logs.clear();
      this.events.clear();
    }
  }

  sweep(windowMsByKey, now = Date.now()) {
    for (const key of this.logs.keys()) {
      const windowMs = windowMsByKey.get(key) || DEFAULT_WINDOW_MS;
      if (this.prune(key, windowMs, now).length === 0) this.logs.delete(key);
    }
  }
}

const rateStore = new SlidingWindowLog();

/* -----------------------------------------------------------------------------
 * 5. API key resolution with a small read-through cache
 * ---------------------------------------------------------------------------*/
const keyCache = new Map(); // key -> { doc, fetchedAt }

function toKeyInfo(doc) {
  return {
    key: doc.key,
    label: doc.label,
    tier: doc.tier,
    limitPerWindow: doc.limitPerWindow,
    windowMs: doc.windowMs || DEFAULT_WINDOW_MS,
    active: doc.active !== false,
  };
}

async function resolveApiKey(rawKey) {
  const cached = keyCache.get(rawKey);
  if (cached && Date.now() - cached.fetchedAt < KEY_CACHE_TTL_MS) return cached.doc;
  const doc = await ApiKey.findOne({ key: rawKey }).lean();
  if (!doc) {
    keyCache.delete(rawKey);
    return null;
  }
  const info = toKeyInfo(doc);
  keyCache.set(rawKey, { doc: info, fetchedAt: Date.now() });
  return info;
}

/* -----------------------------------------------------------------------------
 * 6. Gateway middleware pipeline: Ingress Auth -> Sliding-Window Rate Check
 * ---------------------------------------------------------------------------*/
const asyncHandler = (fn) => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);

function extractBearer(header) {
  if (!header) return null;
  const m = /^Bearer\s+(.+)$/i.exec(header);
  return m ? m[1] : null;
}

/** Stage 1: identify the caller from the x-api-key header (or Authorization: Bearer). */
const ingressAuth = asyncHandler(async (req, res, next) => {
  const start = performance.now();
  req.trace = { start, authMs: 0, rateCheckMs: 0 };

  const raw = (req.get('x-api-key') || extractBearer(req.get('authorization')) || '').trim();
  if (!raw) {
    return res.status(401).json({
      error: 'MISSING_API_KEY',
      message: 'Send your API key in the x-api-key header.',
      pipeline: { authMs: round(performance.now() - start, 3) },
    });
  }

  const keyInfo = await resolveApiKey(raw);
  if (!keyInfo) {
    return res.status(401).json({
      error: 'INVALID_API_KEY',
      message: 'This API key is not registered with the gateway.',
      pipeline: { authMs: round(performance.now() - start, 3) },
    });
  }
  if (!keyInfo.active) {
    return res.status(403).json({
      error: 'API_KEY_DISABLED',
      message: 'This API key has been disabled.',
      pipeline: { authMs: round(performance.now() - start, 3) },
    });
  }

  req.apiKey = keyInfo;
  req.trace.authMs = performance.now() - start;
  return next();
});

function setRateHeaders(res, decision) {
  res.set('X-RateLimit-Limit', String(decision.limit));
  res.set('X-RateLimit-Remaining', String(decision.remaining));
  res.set('X-RateLimit-Reset', String(Math.max(0, Math.ceil(decision.resetMs / 1000))));
}

/** Stage 2: Sliding-Window Log admission control. */
function slidingWindowLimiter(req, res, next) {
  const start = performance.now();
  const { key, limitPerWindow, windowMs, tier } = req.apiKey;
  const decision = rateStore.tryConsume(key, limitPerWindow, windowMs);
  req.trace.rateCheckMs = performance.now() - start;
  setRateHeaders(res, decision);

  if (!decision.allowed) {
    const retryAfterSeconds = Math.max(1, Math.ceil(decision.retryAfterMs / 1000));
    res.set('Retry-After', String(retryAfterSeconds));
    rateStore.recordEvent(key, 429);
    return res.status(429).json({
      error: 'RATE_LIMIT_EXCEEDED',
      message: `Quota of ${limitPerWindow} requests per ${Math.round(windowMs / 1000)}s exhausted for the ${tier} tier.`,
      tier,
      limit: limitPerWindow,
      windowSeconds: Math.round(windowMs / 1000),
      retryAfterSeconds,
      pipeline: {
        authMs: round(req.trace.authMs, 3),
        rateCheckMs: round(req.trace.rateCheckMs, 3),
      },
    });
  }

  req.rateDecision = decision;
  // Record the final status of admitted requests (200, 400, 500) once the response is sent.
  res.on('finish', () => rateStore.recordEvent(key, res.statusCode));
  return next();
}

/* -----------------------------------------------------------------------------
 * 7. Lexical retrieval: Mongo inverted-index lookup + chunk-level BM25 ranking
 * ---------------------------------------------------------------------------*/
async function lexicalSearch(terms) {
  const termSet = new Set(terms);
  const titleRegex = new RegExp(`\\b(${terms.map(escapeRegex).join('|')})`, 'i');

  const [candidates, totalDocs] = await Promise.all([
    KnowledgeDocument.find({
      $or: [{ tokens: { $in: terms } }, { title: { $regex: titleRegex } }],
    })
      .select('title docType tags chunks tokens createdAt')
      .limit(MAX_CANDIDATES)
      .lean(),
    KnowledgeDocument.estimatedDocumentCount(),
  ]);

  if (!candidates.length) return { results: [], totalCandidates: 0 };

  // Document frequency and BM25 inverse document frequency per term.
  const N = Math.max(totalDocs, candidates.length);
  const df = new Map(terms.map((t) => [t, 0]));
  for (const doc of candidates) {
    const tokenSet = new Set(doc.tokens);
    for (const t of terms) if (tokenSet.has(t)) df.set(t, df.get(t) + 1);
  }
  const idf = new Map(terms.map((t) => [t, Math.log(1 + (N - df.get(t) + 0.5) / (df.get(t) + 0.5))]));

  // Tokenise every candidate chunk once; compute the average chunk length.
  const prepared = candidates.map((doc) => ({
    doc,
    chunks: (doc.chunks || []).map((c) => ({ index: c.index, text: c.text, toks: tokenize(c.text) })),
  }));
  let totalLen = 0;
  let chunkCount = 0;
  for (const p of prepared) {
    for (const c of p.chunks) {
      totalLen += c.toks.length;
      chunkCount += 1;
    }
  }
  const avgLen = chunkCount ? totalLen / chunkCount : 1;

  const scored = prepared.map(({ doc, chunks }) => {
    const matched = new Set();

    const scoredChunks = chunks.map((ch) => {
      const tf = new Map();
      for (const tok of ch.toks) if (termSet.has(tok)) tf.set(tok, (tf.get(tok) || 0) + 1);
      let score = 0;
      for (const [t, f] of tf) {
        matched.add(t);
        const norm = (f * (BM25_K1 + 1)) / (f + BM25_K1 * (1 - BM25_B + BM25_B * (ch.toks.length / avgLen)));
        score += idf.get(t) * norm;
      }
      return { index: ch.index, text: ch.text, score };
    });

    const titleTokens = new Set(tokenize(doc.title));
    const tagTokens = new Set(tokenize((doc.tags || []).join(' ')));
    let fieldBoost = 0;
    for (const t of terms) {
      if (titleTokens.has(t)) {
        fieldBoost += TITLE_BOOST * idf.get(t);
        matched.add(t);
      }
      if (tagTokens.has(t)) {
        fieldBoost += TAG_BOOST * idf.get(t);
        matched.add(t);
      }
    }

    const best = scoredChunks.reduce((m, c) => Math.max(m, c.score), 0);
    const coverage = matched.size / terms.length;
    const score = (best + fieldBoost) * (0.5 + 0.5 * coverage);

    let top = scoredChunks
      .filter((c) => c.score > 0)
      .sort((a, b) => b.score - a.score)
      .slice(0, MAX_CHUNKS_PER_RESULT);
    if (!top.length && scoredChunks.length) top = [scoredChunks[0]];

    return {
      id: String(doc._id),
      title: doc.title,
      titleSegments: highlight(doc.title, termSet),
      docType: doc.docType,
      tags: doc.tags || [],
      score: round(score, 3),
      coverage: round(coverage, 2),
      matchedTerms: terms.filter((t) => matched.has(t)),
      chunks: top.map((c) => ({
        index: c.index,
        score: round(c.score, 3),
        segments: highlight(c.text, termSet),
      })),
    };
  });

  const results = scored
    .filter((r) => r.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, MAX_RESULTS);

  return { results, totalCandidates: candidates.length };
}

/* -----------------------------------------------------------------------------
 * 8. Express application and routes
 * ---------------------------------------------------------------------------*/
const app = express();

app.disable('x-powered-by');
app.use(
  cors({
    origin: true,
    exposedHeaders: ['X-RateLimit-Limit', 'X-RateLimit-Remaining', 'X-RateLimit-Reset', 'Retry-After'],
  }),
);
app.use(express.json({ limit: '100kb' }));

function toPublicDoc(doc) {
  const content = doc.content || '';
  return {
    id: String(doc._id),
    title: doc.title,
    docType: doc.docType,
    tags: doc.tags || [],
    wordCount: doc.wordCount || 0,
    chunkCount: (doc.chunks || []).length,
    preview: content.length > 180 ? `${content.slice(0, 177)}...` : content,
    createdAt: doc.createdAt,
  };
}

// Health probe used by the UI's gateway status pill.
app.get('/api/health', asyncHandler(async (req, res) => {
  const states = ['disconnected', 'connected', 'connecting', 'disconnecting'];
  const mongoState = states[mongoose.connection.readyState] || 'unknown';
  const documents = mongoState === 'connected' ? await KnowledgeDocument.estimatedDocumentCount() : 0;
  res.json({
    status: 'ok',
    mongo: mongoState,
    uptimeSeconds: Math.round(process.uptime()),
    documents,
  });
}));

// Public key catalogue (demo only: real gateways never list secrets).
app.get('/api/keys', asyncHandler(async (req, res) => {
  const keys = await ApiKey.find({ active: true }).sort({ limitPerWindow: 1 }).lean();
  res.json(keys.map((k) => ({
    key: k.key,
    label: k.label,
    tier: k.tier,
    limit: k.limitPerWindow,
    windowSeconds: Math.round((k.windowMs || DEFAULT_WINDOW_MS) / 1000),
  })));
}));

// Protected search endpoint: Ingress Auth -> Rate Check -> Mongo Retrieval.
app.post('/api/search', ingressAuth, slidingWindowLimiter, asyncHandler(async (req, res) => {
  const query = typeof (req.body && req.body.query) === 'string' ? req.body.query.trim() : '';
  if (!query) {
    return res.status(400).json({ error: 'EMPTY_QUERY', message: 'Body must contain a non-empty "query" string.' });
  }
  if (query.length > MAX_QUERY_LENGTH) {
    return res.status(400).json({ error: 'QUERY_TOO_LONG', message: `Query must be at most ${MAX_QUERY_LENGTH} characters.` });
  }
  const terms = uniqueTokens(query);
  if (!terms.length) {
    return res.status(400).json({ error: 'NO_SEARCHABLE_TERMS', message: 'The query contains only stop-words or punctuation.' });
  }

  const retrievalStart = performance.now();
  const { results, totalCandidates } = await lexicalSearch(terms);
  const retrievalMs = performance.now() - retrievalStart;
  const d = req.rateDecision;

  return res.json({
    query,
    tokens: terms,
    results,
    totalCandidates,
    pipeline: {
      authMs: round(req.trace.authMs, 3),
      rateCheckMs: round(req.trace.rateCheckMs, 3),
      retrievalMs: round(retrievalMs, 3),
      totalMs: round(performance.now() - req.trace.start, 3),
    },
    quota: {
      tier: req.apiKey.tier,
      limit: d.limit,
      used: d.used,
      remaining: d.remaining,
      resetSeconds: Math.ceil(d.resetMs / 1000),
    },
  });
}));

// Quota introspection (not rate limited, so the dashboard can poll it).
app.get('/api/quota/:key', asyncHandler(async (req, res) => {
  const keyInfo = await resolveApiKey(String(req.params.key || '').trim());
  if (!keyInfo) {
    return res.status(404).json({ error: 'UNKNOWN_API_KEY', message: 'No such API key.' });
  }
  const now = Date.now();
  const snap = rateStore.snapshot(keyInfo.key, keyInfo.limitPerWindow, keyInfo.windowMs, now);
  return res.json({
    key: keyInfo.key,
    label: keyInfo.label,
    tier: keyInfo.tier,
    limit: keyInfo.limitPerWindow,
    used: snap.used,
    remaining: snap.remaining,
    windowSeconds: Math.round(keyInfo.windowMs / 1000),
    resetInSeconds: Math.ceil(snap.resetInMs / 1000),
    retryAfterSeconds: Math.ceil(snap.retryAfterMs / 1000),
    timestamps: snap.timestamps,
    events: snap.events,
    serverNow: now,
  });
}));

// Reset one key's window (body: { key }) or every key (empty body).
app.post('/api/reset-quota', asyncHandler(async (req, res) => {
  const key = req.body && typeof req.body.key === 'string' ? req.body.key.trim() : '';
  if (key) {
    const keyInfo = await resolveApiKey(key);
    if (!keyInfo) return res.status(404).json({ error: 'UNKNOWN_API_KEY', message: 'No such API key.' });
    rateStore.reset(key);
    return res.json({ ok: true, scope: 'key', key, message: `Quota restored for ${keyInfo.label}.` });
  }
  rateStore.reset();
  return res.json({ ok: true, scope: 'all', message: 'Quota restored for every key.' });
}));

// Document catalogue.
app.get('/api/documents', asyncHandler(async (req, res) => {
  const docs = await KnowledgeDocument.find()
    .select('title docType tags wordCount chunks content createdAt')
    .sort({ createdAt: -1 })
    .lean();
  res.json(docs.map(toPublicDoc));
}));

app.post('/api/documents', asyncHandler(async (req, res) => {
  const body = req.body || {};
  const title = typeof body.title === 'string' ? body.title.trim() : '';
  const content = typeof body.content === 'string' ? body.content.trim() : '';
  const docType = typeof body.docType === 'string' ? body.docType.trim().toUpperCase() : 'NOTE';

  const errors = [];
  if (title.length < 3 || title.length > 160) errors.push('title must be 3-160 characters');
  if (content.length < 20 || content.length > 20000) errors.push('content must be 20-20000 characters');
  if (!DOC_TYPES.includes(docType)) errors.push(`docType must be one of ${DOC_TYPES.join(', ')}`);
  if (errors.length) {
    return res.status(400).json({ error: 'VALIDATION_FAILED', message: errors.join('; ') });
  }

  const created = await KnowledgeDocument.create({
    title,
    content,
    docType,
    tags: normalizeTags(body.tags),
  });
  return res.status(201).json(toPublicDoc(created.toObject()));
}));

app.delete('/api/documents/:id', asyncHandler(async (req, res) => {
  if (!mongoose.isValidObjectId(req.params.id)) {
    return res.status(400).json({ error: 'INVALID_ID', message: 'Malformed document id.' });
  }
  const deleted = await KnowledgeDocument.findByIdAndDelete(req.params.id).lean();
  if (!deleted) return res.status(404).json({ error: 'NOT_FOUND', message: 'Document not found.' });
  return res.json({ ok: true, id: req.params.id });
}));

// 404 for unknown API routes.
app.use((req, res) => {
  res.status(404).json({ error: 'ROUTE_NOT_FOUND', message: `${req.method} ${req.originalUrl} does not exist.` });
});

// Central error handler.
// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => {
  if (err && err.type === 'entity.parse.failed') {
    return res.status(400).json({ error: 'MALFORMED_JSON', message: 'Request body is not valid JSON.' });
  }
  if (err instanceof mongoose.Error.ValidationError) {
    return res.status(400).json({ error: 'VALIDATION_FAILED', message: err.message });
  }
  console.error('[gateway] unhandled error:', err);
  return res.status(500).json({ error: 'INTERNAL_ERROR', message: 'Unexpected server error.' });
});

/* -----------------------------------------------------------------------------
 * 9. Bootstrap: connect, seed, listen
 * ---------------------------------------------------------------------------*/
async function seedDatabase() {
  for (const k of DEFAULT_API_KEYS) {
    const { key, ...rest } = k;
    await ApiKey.updateOne({ key }, { $setOnInsert: rest }, { upsert: true });
  }

  const docCount = await KnowledgeDocument.countDocuments();
  if (docCount === 0) {
    // create() runs the pre-validate hook, so chunks and tokens are derived.
    await KnowledgeDocument.create(SEED_DOCUMENTS);
    console.log(`[seed] inserted ${SEED_DOCUMENTS.length} knowledge documents`);
  } else {
    // Back-fill derived fields for documents created outside this server.
    const stale = await KnowledgeDocument.find({ $or: [{ tokens: { $size: 0 } }, { chunks: { $size: 0 } }] });
    for (const doc of stale) await doc.save();
  }

  const keys = await ApiKey.find().lean();
  for (const k of keys) keyCache.set(k.key, { doc: toKeyInfo(k), fetchedAt: Date.now() });
  console.log(`[seed] ${keys.length} API keys ready: ${keys.map((k) => `${k.label} (${k.limitPerWindow}/min)`).join(', ')}`);
}

async function start() {
  mongoose.set('strictQuery', true);
  try {
    await mongoose.connect(MONGO_URI, { serverSelectionTimeoutMS: 5000 });
  } catch (err) {
    console.error(`[mongo] could not connect to ${MONGO_URI}`);
    console.error('[mongo] start MongoDB locally (mongod or Docker) and try again.');
    console.error(`[mongo] ${err.message}`);
    process.exit(1);
  }
  console.log(`[mongo] connected to ${MONGO_URI}`);
  await KnowledgeDocument.syncIndexes();
  await seedDatabase();

  // Periodically drop empty windows so idle keys do not hold memory.
  setInterval(() => {
    const windows = new Map([...keyCache.values()].map(({ doc }) => [doc.key, doc.windowMs]));
    rateStore.sweep(windows);
  }, DEFAULT_WINDOW_MS).unref();

  app.listen(PORT, () => {
    console.log(`[gateway] listening on http://localhost:${PORT}`);
    console.log('[gateway] POST /api/search is protected by the sliding-window limiter');
  });
}

async function shutdown(signal) {
  console.log(`[gateway] ${signal} received, closing MongoDB connection`);
  await mongoose.connection.close();
  process.exit(0);
}
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

start();
