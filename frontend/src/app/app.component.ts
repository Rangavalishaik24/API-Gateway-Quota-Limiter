import { Component, OnDestroy, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { HttpClient, HttpClientModule, HttpErrorResponse, HttpHeaders } from '@angular/common/http';
import { Observable, Subject, Subscription, firstValueFrom, forkJoin, interval, merge, of } from 'rxjs';
import { catchError, map, startWith, switchMap } from 'rxjs/operators';

/* ---------------------------------------------------------------------------
 * Configuration
 * ------------------------------------------------------------------------- */
const API_BASE = 'http://localhost:3000/api';
const POLL_INTERVAL_MS = 2000;   // quota + health sync cadence
const TICK_MS = 250;             // local clock for smooth countdowns between polls
const SPIKE_REQUESTS = 8;
const SPIKE_GAP_MS = 120;
const SPIKE_QUERIES = [
  'oauth token rotation',
  'disaster recovery snapshot',
  'p99 latency budget',
  'zero trust mtls',
  'refresh token replay',
  'replica set failover',
  'circuit breaker retries',
  'audit trail anomalies',
];

/* ---------------------------------------------------------------------------
 * Types
 * ------------------------------------------------------------------------- */
type TabId = 'console' | 'radar' | 'vault';
type StepId = 'auth' | 'rate' | 'mongo';
type StepState = 'idle' | 'active' | 'done' | 'failed' | 'skipped';
type Tier = 'FREE' | 'ENTERPRISE_PRO';
type GatewayState = 'CONNECTING' | 'ONLINE' | 'THROTTLED' | 'DEGRADED' | 'OFFLINE';

export interface ApiKeyInfo {
  key: string;
  label: string;
  tier: Tier;
  limit: number;
  windowSeconds: number;
}

export interface Segment {
  text: string;
  hit: boolean;
}

export interface ChunkHit {
  index: number;
  score: number;
  segments: Segment[];
}

export interface SearchResult {
  id: string;
  title: string;
  titleSegments: Segment[];
  docType: string;
  tags: string[];
  score: number;
  coverage: number;
  matchedTerms: string[];
  chunks: ChunkHit[];
}

export interface PipelineTiming {
  authMs: number;
  rateCheckMs: number;
  retrievalMs: number;
  totalMs: number;
}

export interface SearchResponse {
  query: string;
  tokens: string[];
  results: SearchResult[];
  totalCandidates: number;
  pipeline: PipelineTiming;
  quota: { tier: Tier; limit: number; used: number; remaining: number; resetSeconds: number };
}

export interface ApiErrorBody {
  error: string;
  message: string;
  retryAfterSeconds?: number;
  pipeline?: Partial<PipelineTiming>;
}

export interface GatewayEvent {
  at: number;
  status: number;
}

export interface QuotaSnapshot {
  key: string;
  label: string;
  tier: Tier;
  limit: number;
  used: number;
  remaining: number;
  windowSeconds: number;
  resetInSeconds: number;
  retryAfterSeconds: number;
  timestamps: number[];
  events: GatewayEvent[];
  serverNow: number;
}

export interface HealthResponse {
  status: string;
  mongo: string;
  uptimeSeconds: number;
  documents: number;
}

export interface KnowledgeDoc {
  id: string;
  title: string;
  docType: string;
  tags: string[];
  wordCount: number;
  chunkCount: number;
  preview: string;
  createdAt: string;
}

export interface TimelineMark {
  t: number;
  pos: number; // 0 = left edge (60 s ago), 100 = right edge (now)
}

export interface LiveQuota {
  limit: number;
  used: number;
  remaining: number;
  ratio: number;
  nextFreeIn: number;
  fullResetIn: number;
  windowSeconds: number;
  accepted: TimelineMark[];
  rejected: TimelineMark[];
  synced: boolean;
}

export interface PipelineStep {
  id: StepId;
  label: string;
  detail: string;
  state: StepState;
  ms: number | null;
  note: string;
}

export interface RateHeaders {
  limit: number;
  remaining: number;
  reset: number;
}

interface CallOutcome {
  status: number;
  latencyMs: number;
  body: SearchResponse | null;
  error: ApiErrorBody | null;
  retryAfter: number;
  headers: RateHeaders | null;
}

export interface SpikeEntry {
  n: number;
  status: number;
  latencyMs: number;
  message: string;
}

export interface UiError {
  status: number;
  title: string;
  message: string;
}

export interface Toast {
  kind: 'ok' | 'error';
  text: string;
}

const FALLBACK_KEYS: ApiKeyInfo[] = [
  { key: 'free_tier_demo_key_5rpm', label: 'Free Tier Key', tier: 'FREE', limit: 5, windowSeconds: 60 },
  { key: 'enterprise_pro_demo_key_15rpm', label: 'Enterprise Pro Key', tier: 'ENTERPRISE_PRO', limit: 15, windowSeconds: 60 },
];

function isApiErrorBody(value: unknown): value is ApiErrorBody {
  return typeof value === 'object' && value !== null && 'error' in value && 'message' in value;
}

/* ---------------------------------------------------------------------------
 * Component
 * ------------------------------------------------------------------------- */
@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, FormsModule, HttpClientModule],
  templateUrl: './app.component.html',
})
export class AppComponent implements OnInit, OnDestroy {
  readonly apiBase = API_BASE;
  readonly spikeCount = SPIKE_REQUESTS;
  readonly gaugeCircumference = 2 * Math.PI * 88;
  readonly windowCircumference = 2 * Math.PI * 70;
  readonly docTypes = ['POLICY', 'PROTOCOL', 'SLA', 'GUIDELINE', 'NOTE'];
  readonly suggestions = ['refresh token rotation', 'disaster recovery oplog', 'p95 latency', 'zero trust gateway'];
  readonly timelineTicks = [0, 1, 2, 3, 4, 5, 6];

  readonly tabs: { id: TabId; label: string; hint: string }[] = [
    { id: 'console', label: 'Developer Console & Lexical Search', hint: 'Query the knowledge base through the gateway' },
    { id: 'radar', label: 'Gateway Quota & Rate Limit Radar', hint: 'Watch the sliding window fill and drain' },
    { id: 'vault', label: 'Knowledge Base & Chunks Vault', hint: 'Add and inspect stored documents' },
  ];

  activeTab: TabId = 'console';

  // Keys and quota state (shared by every tab)
  keys: ApiKeyInfo[] = FALLBACK_KEYS;
  activeKey = FALLBACK_KEYS[0].key;
  quotas: Record<string, QuotaSnapshot> = {};
  live: Record<string, LiveQuota> = {};
  clockOffset = 0;
  health: HealthResponse | null = null;
  pollCount = 0;
  lastSync: number | null = null;

  // Tab 1: search
  query = '';
  searching = false;
  searchResponse: SearchResponse | null = null;
  searchError: UiError | null = null;
  lastRoundTripMs: number | null = null;
  lastHeaders: RateHeaders | null = null;
  steps: PipelineStep[] = this.freshSteps();

  // 429 feedback
  retryUntil = 0;
  retryTotal = 0;
  retrySeconds = 0;
  last429Label = '';
  last429Key = '';
  shaking = false;
  flashing = false;

  // Tab 2: spike + reset
  spikeRunning = false;
  spikeLog: SpikeEntry[] = [];
  resetting = false;

  // Tab 3: vault
  documents: KnowledgeDoc[] = [];
  docsLoading = false;
  docsError: string | null = null;
  docFilter = '';
  newDoc = { title: '', docType: 'GUIDELINE', tags: '', content: '' };
  savingDoc = false;
  formError: string | null = null;
  deletingId: string | null = null;

  toast: Toast | null = null;

  private readonly refresh$ = new Subject<void>();
  private readonly subs = new Subscription();
  private pipelineRun = 0;
  private animTimers: ReturnType<typeof setTimeout>[] = [];
  private toastTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(private readonly http: HttpClient) {}

  /* ------------------------------------------------------------------ lifecycle */

  ngOnInit(): void {
    this.recomputeLive();

    // Local 250 ms clock: drives countdowns and timeline drift between polls.
    this.subs.add(interval(TICK_MS).subscribe(() => this.recomputeLive()));

    // RxJS polling every 2 s (plus on-demand refreshes after each request).
    this.subs.add(
      merge(interval(POLL_INTERVAL_MS).pipe(startWith(0)), this.refresh$)
        .pipe(switchMap(() => this.fetchGatewayState()))
        .subscribe(({ health, quotas }) => this.applyGatewayState(health, quotas)),
    );

    this.loadKeys();
    this.loadDocuments();
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
    this.animTimers.forEach((t) => clearTimeout(t));
    if (this.toastTimer) clearTimeout(this.toastTimer);
    this.pipelineRun += 1;
  }

  /* ------------------------------------------------------------------ navigation */

  go(tab: TabId): void {
    if (this.activeTab === tab) return;
    this.activeTab = tab;
    if (tab === 'vault') this.loadDocuments();
  }

  selectKey(key: string): void {
    if (this.activeKey === key) return;
    this.activeKey = key;
    this.searchError = null;
    this.lastHeaders = null;
  }

  /* ------------------------------------------------------------------ polling */

  private fetchGatewayState(): Observable<{ health: HealthResponse | null; quotas: (QuotaSnapshot | null)[] }> {
    const health$ = this.http.get<HealthResponse>(`${API_BASE}/health`).pipe(catchError(() => of(null)));
    const quotas$ = this.keys.length
      ? forkJoin(
          this.keys.map((k) =>
            this.http
              .get<QuotaSnapshot>(`${API_BASE}/quota/${encodeURIComponent(k.key)}`)
              .pipe(catchError(() => of(null))),
          ),
        )
      : of([] as (QuotaSnapshot | null)[]);
    return forkJoin({ health: health$, quotas: quotas$ });
  }

  private applyGatewayState(health: HealthResponse | null, quotas: (QuotaSnapshot | null)[]): void {
    const receivedAt = Date.now();
    this.pollCount += 1;
    this.health = health;
    const next = { ...this.quotas };
    for (const q of quotas) {
      if (!q) continue;
      next[q.key] = q;
      this.clockOffset = q.serverNow - receivedAt;
    }
    this.quotas = next;
    if (health) this.lastSync = receivedAt;
    this.recomputeLive();
  }

  private loadKeys(): void {
    this.http
      .get<ApiKeyInfo[]>(`${API_BASE}/keys`)
      .pipe(catchError(() => of(null)))
      .subscribe((keys) => {
        if (keys && keys.length) {
          this.keys = keys;
          if (!keys.some((k) => k.key === this.activeKey)) this.activeKey = keys[0].key;
          this.refresh$.next();
        }
      });
  }

  /** Re-derive live quota numbers from the last snapshot using the server-aligned clock. */
  private recomputeLive(): void {
    const now = Date.now() + this.clockOffset;
    const next: Record<string, LiveQuota> = {};
    for (const k of this.keys) {
      const snap = this.quotas[k.key];
      const windowSeconds = snap ? snap.windowSeconds : k.windowSeconds;
      const windowMs = windowSeconds * 1000;
      const limit = snap ? snap.limit : k.limit;
      const accepted = snap ? snap.timestamps.filter((t) => t > now - windowMs) : [];
      const rejected = snap ? snap.events.filter((e) => e.status === 429 && e.at > now - windowMs).map((e) => e.at) : [];
      const used = Math.min(accepted.length, limit);
      const pos = (t: number): number => Math.min(100, Math.max(0, 100 - ((now - t) / windowMs) * 100));
      next[k.key] = {
        limit,
        used,
        remaining: Math.max(0, limit - used),
        ratio: limit > 0 ? used / limit : 0,
        nextFreeIn: accepted.length ? Math.max(0, Math.ceil((accepted[0] + windowMs - now) / 1000)) : 0,
        fullResetIn: accepted.length ? Math.max(0, Math.ceil((accepted[accepted.length - 1] + windowMs - now) / 1000)) : 0,
        windowSeconds,
        accepted: accepted.map((t) => ({ t, pos: pos(t) })),
        rejected: rejected.map((t) => ({ t, pos: pos(t) })),
        synced: !!snap,
      };
    }
    this.live = next;
    this.retrySeconds = Math.max(0, Math.ceil((this.retryUntil - Date.now()) / 1000));
  }

  /* ------------------------------------------------------------------ derived view state */

  get activeKeyInfo(): ApiKeyInfo {
    return this.keys.find((k) => k.key === this.activeKey) ?? this.keys[0];
  }

  get activeLive(): LiveQuota {
    return this.liveFor(this.activeKeyInfo);
  }

  liveFor(k: ApiKeyInfo): LiveQuota {
    return (
      this.live[k.key] ?? {
        limit: k.limit,
        used: 0,
        remaining: k.limit,
        ratio: 0,
        nextFreeIn: 0,
        fullResetIn: 0,
        windowSeconds: k.windowSeconds,
        accepted: [],
        rejected: [],
        synced: false,
      }
    );
  }

  get gatewayState(): GatewayState {
    if (!this.health) return this.pollCount === 0 ? 'CONNECTING' : 'OFFLINE';
    if (this.health.mongo !== 'connected') return 'DEGRADED';
    if (this.activeLive.synced && this.activeLive.remaining === 0) return 'THROTTLED';
    return 'ONLINE';
  }

  get statusPillClass(): string {
    switch (this.gatewayState) {
      case 'ONLINE':
        return 'border-emerald-500/50 bg-emerald-500/10 text-emerald-300';
      case 'THROTTLED':
        return 'border-red-500/60 bg-red-500/10 text-red-300 crimson-breathe';
      case 'DEGRADED':
        return 'border-amber-500/60 bg-amber-500/10 text-amber-300';
      case 'OFFLINE':
        return 'border-red-500/60 bg-red-500/10 text-red-300';
      default:
        return 'border-indigo-500/50 bg-indigo-500/10 text-indigo-300';
    }
  }

  get statusDotClass(): string {
    switch (this.gatewayState) {
      case 'ONLINE':
        return 'bg-emerald-400';
      case 'DEGRADED':
        return 'bg-amber-400';
      case 'CONNECTING':
        return 'bg-indigo-400';
      default:
        return 'bg-red-500';
    }
  }

  get statusLabel(): string {
    switch (this.gatewayState) {
      case 'ONLINE':
        return 'Gateway online';
      case 'THROTTLED':
        return 'Throttling this key';
      case 'DEGRADED':
        return 'MongoDB disconnected';
      case 'OFFLINE':
        return 'Gateway offline';
      default:
        return 'Connecting…';
    }
  }

  quotaColor(l: LiveQuota): string {
    if (l.ratio >= 1) return '#EF4444';
    if (l.ratio >= 0.6) return '#F59E0B';
    return '#10B981';
  }

  barClass(l: LiveQuota): string {
    if (l.ratio >= 1) return 'bg-red-500';
    if (l.ratio >= 0.6) return 'bg-amber-500';
    return 'bg-emerald-500';
  }

  gaugeOffset(l: LiveQuota): number {
    return this.gaugeCircumference * (1 - Math.min(1, l.ratio));
  }

  windowOffset(l: LiveQuota): number {
    if (!l.accepted.length || l.windowSeconds <= 0) return this.windowCircumference;
    const fraction = Math.min(1, l.nextFreeIn / l.windowSeconds);
    return this.windowCircumference * (1 - fraction);
  }

  /** Seconds until the active key admits traffic again, from server state or the last 429. */
  get radarRetry(): number {
    const l = this.activeLive;
    if (l.synced && l.remaining === 0) return Math.max(1, l.nextFreeIn);
    if (this.retrySeconds > 0 && this.last429Key === this.activeKey) return this.retrySeconds;
    return 0;
  }

  get radarRetryProgress(): number {
    const total = this.retryTotal > 0 && this.last429Key === this.activeKey ? this.retryTotal : this.activeLive.windowSeconds;
    return total > 0 ? Math.min(100, (this.radarRetry / total) * 100) : 0;
  }

  stepClass(state: StepState): string {
    switch (state) {
      case 'active':
        return 'border-indigo-500/70 bg-indigo-500/10 shadow-glow';
      case 'done':
        return 'border-emerald-500/50 bg-emerald-500/[0.06]';
      case 'failed':
        return 'border-red-500/70 bg-red-500/10 shadow-glow-crimson';
      case 'skipped':
        return 'border-slate-800 bg-obsidian-950/40 opacity-50';
      default:
        return 'border-slate-800 bg-obsidian-950/60';
    }
  }

  connectorClass(state: StepState): string {
    if (state === 'done') return 'bg-emerald-500/70';
    if (state === 'failed') return 'bg-red-500/70';
    return 'bg-slate-700';
  }

  maskKey(key: string): string {
    return key.length > 12 ? `${key.slice(0, 12)}…` : key;
  }

  get spikeOk(): number {
    return this.spikeLog.filter((e) => e.status === 200).length;
  }

  get spikeLimited(): number {
    return this.spikeLog.filter((e) => e.status === 429).length;
  }

  get filteredDocs(): KnowledgeDoc[] {
    const f = this.docFilter.trim().toLowerCase();
    if (!f) return this.documents;
    return this.documents.filter(
      (d) => d.title.toLowerCase().includes(f) || d.tags.some((t) => t.includes(f)) || d.docType.toLowerCase().includes(f),
    );
  }

  get totalWords(): number {
    return this.documents.reduce((sum, d) => sum + d.wordCount, 0);
  }

  get totalChunks(): number {
    return this.documents.reduce((sum, d) => sum + d.chunkCount, 0);
  }

  get newDocWordCount(): number {
    const m = this.newDoc.content.trim().match(/\S+/g);
    return m ? m.length : 0;
  }

  /* ------------------------------------------------------------------ Tab 1: search */

  useSuggestion(s: string): void {
    this.query = s;
    void this.runSearch();
  }

  async runSearch(): Promise<void> {
    const q = this.query.trim();
    if (this.searching) return;
    if (!q) {
      this.searchError = { status: 400, title: 'Type a query first', message: 'Enter at least one keyword, for example "token rotation".' };
      return;
    }

    const run = ++this.pipelineRun;
    this.searching = true;
    this.searchError = null;
    this.searchResponse = null;
    this.steps = this.freshSteps();
    this.setStep('auth', 'active');

    const outcome = await this.callSearch(q, this.activeKey);
    if (run !== this.pipelineRun) return;
    this.lastRoundTripMs = outcome.latencyMs;
    this.lastHeaders = outcome.headers;

    await this.playPipeline(outcome, run);
    if (run !== this.pipelineRun) return;

    this.searching = false;
    if (outcome.status === 200 && outcome.body) {
      this.searchResponse = outcome.body;
    } else {
      this.searchError = this.describeError(outcome);
      if (outcome.status === 429) this.trigger429(outcome.retryAfter, this.activeKey);
    }
    this.refresh$.next();
  }

  /** Replays the server-measured stage timings as a short visual sequence. */
  private async playPipeline(o: CallOutcome, run: number): Promise<void> {
    const alive = (): boolean => run === this.pipelineRun;
    const t: Partial<PipelineTiming> = o.body ? o.body.pipeline : (o.error?.pipeline ?? {});

    await this.sleep(140);
    if (!alive()) return;

    if (o.status === 0) {
      this.setStep('auth', 'failed', null, 'Gateway unreachable');
      this.skipFrom('rate');
      return;
    }
    if (o.status === 401 || o.status === 403) {
      this.setStep('auth', 'failed', t.authMs ?? null, o.error?.error ?? `HTTP ${o.status}`);
      this.skipFrom('rate');
      return;
    }
    this.setStep('auth', 'done', t.authMs ?? null, 'API key recognised');
    this.setStep('rate', 'active');

    await this.sleep(180);
    if (!alive()) return;

    if (o.status === 429) {
      this.setStep('rate', 'failed', t.rateCheckMs ?? null, `Window full, retry in ${o.retryAfter}s`);
      this.skipFrom('mongo');
      return;
    }
    const left = o.headers ? `${o.headers.remaining} of ${o.headers.limit} requests left` : 'Within quota';
    this.setStep('rate', 'done', t.rateCheckMs ?? null, left);
    this.setStep('mongo', 'active');

    await this.sleep(200);
    if (!alive()) return;

    if (o.status === 200 && o.body) {
      this.setStep(
        'mongo',
        'done',
        o.body.pipeline.retrievalMs,
        `${o.body.results.length} ranked of ${o.body.totalCandidates} candidates`,
      );
    } else {
      this.setStep('mongo', 'failed', t.retrievalMs ?? null, o.error?.error ?? `HTTP ${o.status}`);
    }
  }

  private freshSteps(): PipelineStep[] {
    return [
      { id: 'auth', label: 'Ingress auth', detail: 'Resolve x-api-key against MongoDB', state: 'idle', ms: null, note: 'Waiting' },
      { id: 'rate', label: 'Rate check', detail: 'Sliding-window log admission', state: 'idle', ms: null, note: 'Waiting' },
      { id: 'mongo', label: 'Mongo retrieval', detail: 'Token index lookup + BM25 ranking', state: 'idle', ms: null, note: 'Waiting' },
    ];
  }

  private setStep(id: StepId, state: StepState, ms: number | null = null, note = ''): void {
    this.steps = this.steps.map((s) =>
      s.id === id
        ? { ...s, state, ms: ms ?? s.ms, note: note || (state === 'active' ? 'Running…' : s.note) }
        : s,
    );
  }

  private skipFrom(id: StepId): void {
    const order: StepId[] = ['auth', 'rate', 'mongo'];
    const from = order.indexOf(id);
    this.steps = this.steps.map((s) =>
      order.indexOf(s.id) >= from ? { ...s, state: 'skipped', note: 'Not reached' } : s,
    );
  }

  private describeError(o: CallOutcome): UiError {
    if (o.status === 0) {
      return {
        status: 0,
        title: 'Gateway unreachable',
        message: `No response from ${API_BASE}. Start the backend with "npm start" in the backend folder.`,
      };
    }
    if (o.status === 429) {
      return {
        status: 429,
        title: 'HTTP 429 - Rate Limit Exceeded',
        message: `${o.error?.message ?? 'Too many requests.'} Retry after ${o.retryAfter}s, or switch to the Enterprise Pro key.`,
      };
    }
    return {
      status: o.status,
      title: `HTTP ${o.status}: ${o.error?.error ?? 'Request failed'}`,
      message: o.error?.message ?? 'The gateway rejected the request.',
    };
  }

  /* ------------------------------------------------------------------ shared HTTP call */

  private callSearch(query: string, key: string): Promise<CallOutcome> {
    const started = performance.now();
    const headers = new HttpHeaders({ 'x-api-key': key });
    return firstValueFrom(
      this.http.post<SearchResponse>(`${API_BASE}/search`, { query }, { headers, observe: 'response' }).pipe(
        map(
          (res): CallOutcome => ({
            status: res.status,
            latencyMs: performance.now() - started,
            body: res.body,
            error: null,
            retryAfter: 0,
            headers: this.readRateHeaders(res.headers),
          }),
        ),
        catchError((err: HttpErrorResponse) => {
          const body = isApiErrorBody(err.error) ? err.error : null;
          const headerRetry = Number(err.headers?.get('Retry-After'));
          const retryAfter = Number.isFinite(headerRetry) && headerRetry > 0 ? headerRetry : (body?.retryAfterSeconds ?? 1);
          return of<CallOutcome>({
            status: err.status,
            latencyMs: performance.now() - started,
            body: null,
            error: body,
            retryAfter,
            headers: err.headers ? this.readRateHeaders(err.headers) : null,
          });
        }),
      ),
    );
  }

  private readRateHeaders(h: HttpHeaders): RateHeaders | null {
    const limit = h.get('X-RateLimit-Limit');
    if (limit === null) return null;
    return {
      limit: Number(limit),
      remaining: Number(h.get('X-RateLimit-Remaining') ?? 0),
      reset: Number(h.get('X-RateLimit-Reset') ?? 0),
    };
  }

  /* ------------------------------------------------------------------ 429 feedback */

  private trigger429(seconds: number, key: string): void {
    const s = Math.max(1, Math.round(seconds));
    const label = this.keys.find((k) => k.key === key)?.label ?? key;
    this.last429Key = key;
    this.retryTotal = s;
    this.retryUntil = Date.now() + s * 1000;
    this.retrySeconds = s;
    this.last429Label = label;
    this.restartAlarm();
  }

  /** Re-trigger CSS shake/flash even if they are already applied. */
  private restartAlarm(): void {
    this.shaking = false;
    this.flashing = false;
    this.later(() => {
      this.shaking = true;
      this.flashing = true;
    }, 30);
    this.later(() => (this.shaking = false), 620);
    this.later(() => (this.flashing = false), 1200);
  }

  /* ------------------------------------------------------------------ Tab 2: spike + reset */

  async simulateSpike(): Promise<void> {
    if (this.spikeRunning) return;
    this.spikeRunning = true;
    this.spikeLog = [];
    const key = this.activeKey;

    for (let n = 1; n <= SPIKE_REQUESTS; n += 1) {
      const q = SPIKE_QUERIES[(n - 1) % SPIKE_QUERIES.length];
      const o = await this.callSearch(q, key);
      let message: string;
      if (o.status === 200 && o.body) {
        const docs = o.body.results.length;
        const left = o.headers ? `, ${o.headers.remaining} left` : '';
        message = `${docs} doc${docs === 1 ? '' : 's'} for "${q}"${left}`;
      } else if (o.status === 429) {
        message = `Rejected by limiter, Retry-After ${o.retryAfter}s`;
        this.trigger429(o.retryAfter, key);
      } else {
        message = this.describeError(o).title;
      }
      this.spikeLog = [...this.spikeLog, { n, status: o.status, latencyMs: o.latencyMs, message }];
      this.refresh$.next();
      if (o.status === 0) break;
      if (n < SPIKE_REQUESTS) await this.sleep(SPIKE_GAP_MS);
    }
    this.spikeRunning = false;
  }

  resetQuota(allKeys = false): void {
    if (this.resetting) return;
    this.resetting = true;
    const body = allKeys ? {} : { key: this.activeKey };
    this.http.post<{ ok: boolean; message: string }>(`${API_BASE}/reset-quota`, body).subscribe({
      next: (res) => {
        this.resetting = false;
        this.retryUntil = 0;
        this.retrySeconds = 0;
        this.retryTotal = 0;
        if (this.searchError?.status === 429) this.searchError = null;
        this.showToast('ok', res.message);
        this.refresh$.next();
      },
      error: (err: HttpErrorResponse) => {
        this.resetting = false;
        this.showToast('error', err.status === 0 ? 'Gateway unreachable, quota not reset.' : `Reset failed (HTTP ${err.status}).`);
      },
    });
  }

  /* ------------------------------------------------------------------ Tab 3: vault */

  loadDocuments(): void {
    this.docsLoading = true;
    this.docsError = null;
    this.http.get<KnowledgeDoc[]>(`${API_BASE}/documents`).subscribe({
      next: (docs) => {
        this.documents = docs;
        this.docsLoading = false;
      },
      error: (err: HttpErrorResponse) => {
        this.docsLoading = false;
        this.docsError = err.status === 0 ? `Cannot reach ${API_BASE}. Start the backend and retry.` : `Could not load documents (HTTP ${err.status}).`;
      },
    });
  }

  addDocument(): void {
    const title = this.newDoc.title.trim();
    const content = this.newDoc.content.trim();
    if (title.length < 3) {
      this.formError = 'Title needs at least 3 characters.';
      return;
    }
    if (content.length < 20) {
      this.formError = 'Content needs at least 20 characters so it can be chunked.';
      return;
    }
    this.formError = null;
    this.savingDoc = true;
    this.http
      .post<KnowledgeDoc>(`${API_BASE}/documents`, {
        title,
        content,
        docType: this.newDoc.docType,
        tags: this.newDoc.tags,
      })
      .subscribe({
        next: (doc) => {
          this.savingDoc = false;
          this.documents = [doc, ...this.documents];
          this.newDoc = { title: '', docType: this.newDoc.docType, tags: '', content: '' };
          this.showToast('ok', `Saved "${doc.title}" as ${doc.chunkCount} chunk${doc.chunkCount === 1 ? '' : 's'}.`);
        },
        error: (err: HttpErrorResponse) => {
          this.savingDoc = false;
          this.formError = isApiErrorBody(err.error) ? err.error.message : err.status === 0 ? 'Gateway unreachable.' : `Save failed (HTTP ${err.status}).`;
        },
      });
  }

  deleteDocument(doc: KnowledgeDoc): void {
    if (this.deletingId) return;
    if (!window.confirm(`Delete "${doc.title}" from the knowledge base?`)) return;
    this.deletingId = doc.id;
    this.http.delete<{ ok: boolean }>(`${API_BASE}/documents/${encodeURIComponent(doc.id)}`).subscribe({
      next: () => {
        this.deletingId = null;
        this.documents = this.documents.filter((d) => d.id !== doc.id);
        this.showToast('ok', `Deleted "${doc.title}".`);
      },
      error: (err: HttpErrorResponse) => {
        this.deletingId = null;
        this.showToast('error', `Delete failed (HTTP ${err.status}).`);
      },
    });
  }

  /* ------------------------------------------------------------------ helpers */

  private showToast(kind: Toast['kind'], text: string): void {
    this.toast = { kind, text };
    if (this.toastTimer) clearTimeout(this.toastTimer);
    this.toastTimer = setTimeout(() => (this.toast = null), 3200);
  }

  private later(fn: () => void, ms: number): void {
    const id = setTimeout(() => {
      this.animTimers = this.animTimers.filter((t) => t !== id);
      fn();
    }, ms);
    this.animTimers.push(id);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => this.later(resolve, ms));
  }
}
