// pump-fun-paper-sprint — "Young-Coin Lottery"
// A small-stakes, wide-trail paper trader for the pump.fun bonding-curve
// replay (protocol agent-fun/replay/v2).
//
// Why this shape: backtests on the kit's dev window showed that at the
// replay's granularity — one decision per slot, fills two slots later —
// every directional rule (momentum surge, breakout to new highs, trend,
// dip-buying) has zero or negative expectancy after the ~2.5% round-trip
// curve fees, because bursts happen inside a slot and a slot-end decision
// systematically buys the top of a burst and sells the bottom of a crash.
// What DOES survive is the cross-section: buying a young coin at its first
// buyable moment and holding it to the end of the window averages +11% to
// +36% per 6 hours, driven by a few coins that run 2x-13x while the median
// coin drifts -15%. So this bot plays the cross-section, not the tape: it
// buys every coin at its first buyable moment (a few trades in, real
// reserves built enough to allow a 0.01+ SOL order) with a SMALL fixed stake,
// holds with a wide trailing exit so a real runner can pay for the losers,
// and rotates across as many coins as the five-slot cap allows. Small stakes
// keep the bleed per ticket tiny; the wide trail keeps the moonshots.
//
// Determinism: every decision is a pure function of the hello/batch stream.
// No wall clock, no environment reads, no unseeded randomness anywhere, so
// two runs on the same data emit byte-identical replies.
//
// Wire protocol (implemented here from protocol.schema.json, no SDK import):
//   harness -> bot on stdin, one JSON line each: hello, then batch..., then done.
//   bot -> harness on stdout: exactly one {"type":"reply","seq":<seq>,"orders":[...]}
//   line per batch, echoing the batch seq. Diagnostics go to stderr only.

const LAMPORTS_PER_SOL = 1_000_000_000n;
const BPS = 10_000n;

// ---- Tunable strategy parameters (fixed constants; broad, round-number
// ---- risk rules, chosen on the dev window's 6-hour slices, not fitted to
// ---- any single coin).
const TARGET_BUY_LAMPORTS = 30_000_000n; // 0.03 SOL per coin: small lottery stake
const MIN_BUY_LAMPORTS = 10_000_000n; // 0.01 SOL — the minimum-bar round trip
const MIN_TRADES = 5; // need a little tape before trusting a coin
const MIN_REAL_SOL_LAMPORTS = 1_500_000_000n; // 1.5 SOL real reserves (order cap >= 0.015 SOL)
const HARD_STOP = 0.8; // -20% vs entry cost: exit, no exceptions
const TRAIL_DROP = 0.8; // -20% from the peak since entry: exit, lock the run in
const TIME_STOP_SLOTS = 20000; // ~71 min stale: exit unless clearly winning
const TIME_STOP_MIN_GAIN = 1.05; // ...winning means at least +5%
const TOP_UP_WINDOW_SLOTS = 500; // keep topping a fresh entry up for this long
const REENTRY_COOLDOWN_SLOTS = 1500; // do not re-chase a coin just exited
const BRAKE_HALT_EQUITY = 0.7; // below 70% of peak equity: no new entries
const BRAKE_FLATTEN_EQUITY = 0.6; // below 60%: flatten everything
const BUY_SLIPPAGE_BPS = 500; // 5% — fills must survive 2-slot latency
const SELL_SLIPPAGE_BPS = 1000; // 10% — stops must actually execute

// ---- Small helpers ---------------------------------------------------------

function toBig(v: unknown): bigint {
  if (typeof v === "bigint") return v;
  if (typeof v === "number") return BigInt(v);
  return BigInt(String(v));
}

function priceOf(vQuoteReserves: bigint, vTokenReserves: bigint): number {
  if (vTokenReserves <= 0n) return 0;
  // Native ratio: lamports per token base unit. entryPrice below is kept in
  // exactly these units (lamports spent / token base units received), so
  // gain = price / entryPrice is a true ratio. Do NOT rescale either side.
  return Number(vQuoteReserves) / Number(vTokenReserves);
}

// ---- Per-curve tape ---------------------------------------------------------

interface CurveTape {
  mint: string;
  price: number; // latest SOL-per-token price
  realSol: bigint; // latest real SOL reserves (drives the 1% order cap)
  complete: boolean;
  createdSlot: number | null;
  trades: number;
  samples: number[]; // recent prices, oldest first, capped
  recentBuys: number; // buys among the last 10 trades
  recentTrades: number; // trades counted in the dominance window
  buySolRecent: bigint; // buy SOL in the dominance window
  sellSolRecent: bigint;
  recentEvents: Array<{ buy: boolean; sol: bigint }>; // rolling trade window
  lastSeenSlot: number;
}

interface HeldPosition {
  mint: string;
  tokens: bigint;
  costLamports: bigint; // SOL spent on tokens still held (fees included)
  entryPrice: number; // SOL per token at cost
  peakPrice: number; // highest price since entry
  entrySlot: number;
  committedLamports: bigint; // buys sent + filled for this entry (top-up accounting)
  firstBuySlot: number;
}

interface PendingBuy {
  mint: string;
  lamports: bigint;
}

const curves = new Map<string, CurveTape>();
const positions = new Map<string, HeldPosition>();
const pendingBuys = new Map<string, PendingBuy>(); // by client_id
const pendingByMint = new Map<string, bigint>(); // mint -> lamports of buys in flight
const lastExitSlot = new Map<string, number>();

let orderCounter = 0;
function nextClientId(prefix: string): string {
  orderCounter += 1;
  return `${prefix}${orderCounter}`;
}

// ---- Harness state ----------------------------------------------------------

interface HelloState {
  capitalLamports: bigint;
  maxOpenPositions: number;
  maxOrderBps: number;
  maxOrdersPerBatch: number;
  latencySlots: number;
  txFeeLamports: bigint;
}

let hello: HelloState | null = null;
let cashLamports = 0n;
let peakEquityLamports = 0n;
let flattenAll = false;

function curveFor(mint: string, slot: number): CurveTape {
  let c = curves.get(mint);
  if (!c) {
    c = {
      mint,
      price: 0,
      realSol: 0n,
      complete: false,
      createdSlot: null,
      trades: 0,
      samples: [],
      recentBuys: 0,
      recentTrades: 0,
      buySolRecent: 0n,
      sellSolRecent: 0n,
      recentEvents: [],
      lastSeenSlot: slot,
    };
    curves.set(mint, c);
  }
  return c;
}

function notePrice(c: CurveTape, price: number, slot: number): void {
  if (price > 0) {
    c.price = price;
    c.samples.push(price);
    if (c.samples.length > 40) c.samples.shift();
  }
  c.lastSeenSlot = slot;
}

function noteTrade(c: CurveTape, isBuy: boolean, solLamports: bigint): void {
  c.trades += 1;
  c.recentEvents.push({ buy: isBuy, sol: solLamports });
  if (c.recentEvents.length > 10) c.recentEvents.shift();
  let buys = 0;
  let buySol = 0n;
  let sellSol = 0n;
  for (const e of c.recentEvents) {
    if (e.buy) {
      buys += 1;
      buySol += e.sol;
    } else {
      sellSol += e.sol;
    }
  }
  c.recentBuys = buys;
  c.recentTrades = c.recentEvents.length;
  c.buySolRecent = buySol;
  c.sellSolRecent = sellSol;
}

// ---- Equity -----------------------------------------------------------------

function equityLamports(): bigint {
  let total = cashLamports;
  for (const p of positions.values()) {
    const c = curves.get(p.mint);
    const price = c && c.price > 0 ? c.price : p.entryPrice;
    // value = tokens * price (SOL/token) -> lamports
    const value = (p.tokens * BigInt(Math.floor(price * 1e9))) / 1_000_000_000n;
    total += value;
  }
  return total;
}

// ---- Decision ---------------------------------------------------------------

interface WireOrder {
  side: string;
  client_id: string;
  mint: string;
  quote_lamports?: string;
  token_amount?: string;
  max_slippage_bps: number;
}

function decideOrders(batchSlot: number): WireOrder[] {
  const orders: WireOrder[] = [];
  if (!hello) return orders;
  const h = hello;

  // Portfolio brake: flatten if equity sagged hard, halt entries if sagging.
  const equity = equityLamports();
  if (equity > peakEquityLamports) peakEquityLamports = equity;
  const equityRatio =
    peakEquityLamports > 0n ? Number(equity) / Number(peakEquityLamports) : 1;
  flattenAll = equityRatio < BRAKE_FLATTEN_EQUITY;
  const haltEntries = flattenAll || equityRatio < BRAKE_HALT_EQUITY;

  // --- Exits first: hard stop, trailing exit, time stop. No fixed
  // --- take-profit: a runner is left alone until it gives back 20%. ---
  for (const p of [...positions.values()]) {
    const c = curves.get(p.mint);
    if (!c || c.price <= 0) continue;
    if (c.complete) continue; // harness closes completed curves itself
    const price = c.price;
    if (price > p.peakPrice) p.peakPrice = price;
    const gain = price / p.entryPrice;
    const fromPeak = price / p.peakPrice;

    let sell = false;
    if (flattenAll) sell = true;
    else if (gain <= HARD_STOP) sell = true;
    else if (fromPeak <= TRAIL_DROP && p.peakPrice > p.entryPrice) sell = true;
    else if (
      batchSlot - p.entrySlot >= TIME_STOP_SLOTS &&
      gain < TIME_STOP_MIN_GAIN
    ) {
      sell = true;
    }
    if (sell) {
      orders.push({
        side: "sell",
        client_id: nextClientId("s"),
        mint: p.mint,
        token_amount: p.tokens.toString(),
        max_slippage_bps: SELL_SLIPPAGE_BPS,
      });
      lastExitSlot.set(p.mint, batchSlot);
      positions.delete(p.mint);
    }
  }

  // --- Entries: the youngest buyable coins first. A coin qualifies the
  // --- moment it has a little tape and enough real reserves for a
  // --- minimum-bar order, and only while buyers still outspend sellers. ---
  if (!haltEntries && orders.length < h.maxOrdersPerBatch) {
    const openCount =
      positions.size + new Set([...pendingByMint.keys()]).size;
    const freeSlots = Math.max(0, h.maxOpenPositions - openCount);
    if (freeSlots > 0) {
      const candidates: Array<{ mint: string; trades: number }> = [];
      for (const tape of curves.values()) {
        if (tape.complete || tape.trades < MIN_TRADES) continue;
        if (tape.price <= 0 || tape.realSol < MIN_REAL_SOL_LAMPORTS) continue;
        if (positions.has(tape.mint) || pendingByMint.has(tape.mint)) continue;
        const cool = lastExitSlot.get(tape.mint);
        if (cool !== undefined && batchSlot - cool < REENTRY_COOLDOWN_SLOTS) continue;
        if (tape.samples.length < 5) continue;
        if (tape.recentTrades < 5) continue;
        if (tape.buySolRecent <= tape.sellSolRecent) continue; // buyers must lead
        candidates.push({ mint: tape.mint, trades: tape.trades });
      }
      // Youngest first (fewest trades seen); mint string breaks ties.
      candidates.sort((a, b) =>
        a.trades !== b.trades ? a.trades - b.trades : a.mint < b.mint ? -1 : 1,
      );
      for (const cand of candidates.slice(0, freeSlots)) {
        if (orders.length >= h.maxOrdersPerBatch) break;
        const tape = curves.get(cand.mint);
        if (!tape) continue;
        const cap =
          (tape.realSol * BigInt(h.maxOrderBps)) / BPS; // 1% of real reserve
        if (cap < MIN_BUY_LAMPORTS) continue;
        const want = TARGET_BUY_LAMPORTS;
        const spend = want < cap ? want : cap;
        // Never spend cash we do not have (keep one tx fee in reserve).
        const freeCash = cashLamports - h.txFeeLamports;
        if (freeCash <= 0n) break;
        const sized = spend < freeCash ? spend : freeCash;
        if (sized < MIN_BUY_LAMPORTS) continue;
        const id = nextClientId("b");
        orders.push({
          side: "buy",
          client_id: id,
          mint: cand.mint,
          quote_lamports: sized.toString(),
          max_slippage_bps: BUY_SLIPPAGE_BPS,
        });
        pendingBuys.set(id, { mint: cand.mint, lamports: sized });
        pendingByMint.set(
          cand.mint,
          (pendingByMint.get(cand.mint) ?? 0n) + sized,
        );
      }
    }

    // --- Top-ups: a fresh entry capped by the 1% reserve rule grows toward
    // --- the target stake over the next few batches while it still holds up.
    for (const p of [...positions.values()]) {
      if (orders.length >= h.maxOrdersPerBatch) break;
      if (p.committedLamports >= TARGET_BUY_LAMPORTS) continue;
      if (batchSlot - p.firstBuySlot > TOP_UP_WINDOW_SLOTS) continue;
      const c = curves.get(p.mint);
      if (!c || c.complete || c.price <= 0) continue;
      if (c.price < p.entryPrice) continue; // never add to a loser
      if (pendingByMint.has(p.mint)) continue;
      const cap = (c.realSol * BigInt(h.maxOrderBps)) / BPS;
      if (cap < MIN_BUY_LAMPORTS) continue;
      const room = TARGET_BUY_LAMPORTS - p.committedLamports;
      const spend = room < cap ? room : cap;
      const freeCash = cashLamports - h.txFeeLamports;
      if (freeCash <= 0n) break;
      const sized = spend < freeCash ? spend : freeCash;
      if (sized < MIN_BUY_LAMPORTS) continue;
      const id = nextClientId("b");
      orders.push({
        side: "buy",
        client_id: id,
        mint: p.mint,
        quote_lamports: sized.toString(),
        max_slippage_bps: BUY_SLIPPAGE_BPS,
      });
      pendingBuys.set(id, { mint: p.mint, lamports: sized });
      pendingByMint.set(p.mint, (pendingByMint.get(p.mint) ?? 0n) + sized);
    }
  }

  return orders;
}

// ---- Batch application ------------------------------------------------------

function applyBatch(batch: any): void {
  const slot: number = batch.slot;

  // Fills first: clear pending, learn entry prices from buy fills.
  if (Array.isArray(batch.fills)) {
    for (const f of batch.fills) {
      const pend = pendingBuys.get(f.client_id);
      if (pend) {
        pendingBuys.delete(f.client_id);
        const rest = (pendingByMint.get(pend.mint) ?? 0n) - pend.lamports;
        if (rest <= 0n) pendingByMint.delete(pend.mint);
        else pendingByMint.set(pend.mint, rest);
        if (f.status === "filled" && f.side === "buy") {
          const p = positions.get(pend.mint);
          const tokens = toBig(f.token_amount);
          const spent = toBig(f.quote_lamports);
          if (tokens > 0n) {
            if (p) {
              const newTokens = p.tokens + tokens;
              const newCost = p.costLamports + spent;
              p.tokens = newTokens;
              p.costLamports = newCost;
              p.entryPrice = Number(newCost) / Number(newTokens);
              p.committedLamports += spent;
            } else {
              const entryPrice = Number(spent) / Number(tokens);
              positions.set(pend.mint, {
                mint: pend.mint,
                tokens,
                costLamports: spent,
                entryPrice,
                peakPrice: entryPrice,
                entrySlot: slot,
                committedLamports: spent,
                firstBuySlot: slot,
              });
            }
          }
        }
      }
    }
  }

  // Account is truth for cash and holdings.
  if (batch.account) {
    cashLamports = toBig(batch.account.cash_lamports ?? "0");
    const seen = new Set<string>();
    if (Array.isArray(batch.account.positions)) {
      for (const pos of batch.account.positions) {
        const tokens = toBig(pos.token_amount);
        if (tokens <= 0n) continue;
        seen.add(pos.mint);
        const cost = toBig(pos.cost_lamports);
        const p = positions.get(pos.mint);
        if (p) {
          p.tokens = tokens;
          p.costLamports = cost;
          p.entryPrice = Number(cost) / Number(tokens);
        } else {
          const entryPrice = Number(cost) / Number(tokens);
          const c = curves.get(pos.mint);
          positions.set(pos.mint, {
            mint: pos.mint,
            tokens,
            costLamports: cost,
            entryPrice,
            peakPrice: c && c.price > entryPrice ? c.price : entryPrice,
            entrySlot: slot,
            committedLamports: cost,
            firstBuySlot: slot,
          });
        }
      }
    }
    // Positions the harness closed (complete / flatten / writeoff) vanish.
    for (const mint of [...positions.keys()]) {
      if (!seen.has(mint)) {
        positions.delete(mint);
        lastExitSlot.set(mint, slot);
      }
    }
  }

  // Marks refresh curve prices.
  if (Array.isArray(batch.marks)) {
    for (const m of batch.marks) {
      const c = curveFor(m.mint, m.slot);
      if (m.slot < c.lastSeenSlot && c.samples.length > 0) continue;
      const price = priceOf(toBig(m.virtual_quote_reserves), toBig(m.virtual_token_reserves));
      notePrice(c, price, m.slot);
      c.realSol = toBig(m.real_quote_reserves);
    }
  }

  // Events update curves, tape and completion flags.
  if (Array.isArray(batch.events)) {
    for (const e of batch.events) {
      if (e.event === "CreateEvent") {
        const c = curveFor(e.mint, e.slot);
        c.createdSlot = e.slot;
        const price = priceOf(toBig(e.virtual_quote_reserves), toBig(e.virtual_token_reserves));
        notePrice(c, price, e.slot);
        c.realSol = 0n;
      } else if (e.event === "TradeEvent") {
        const c = curveFor(e.mint, e.slot);
        const price = priceOf(toBig(e.virtual_quote_reserves), toBig(e.virtual_token_reserves));
        notePrice(c, price, e.slot);
        c.realSol = toBig(e.real_quote_reserves);
        noteTrade(c, e.is_buy === true, toBig(e.sol_amount));
      } else if (e.event === "CompleteEvent") {
        const c = curveFor(e.mint, e.slot);
        c.complete = true;
      }
    }
  }
}

// ---- Main loop: hello / batch / done over newline-delimited JSON -----------

async function main(): Promise<void> {
  process.stdin.setEncoding("utf8");
  let buffer = "";
  let done = false;

  const handleLine = (line: string): boolean => {
    if (line.trim() === "") return false;
    let msg: any;
    try {
      msg = JSON.parse(line);
    } catch {
      console.error("bad input line, ignoring");
      return false;
    }
    if (msg.type === "hello") {
      hello = {
        capitalLamports: toBig(msg.capital.lamports),
        maxOpenPositions: msg.caps.max_open_positions,
        maxOrderBps: msg.caps.max_order_bps_of_real_reserve,
        maxOrdersPerBatch: msg.caps.max_orders_per_batch,
        latencySlots: msg.latency_slots,
        txFeeLamports:
          toBig(msg.fees.base_fee_lamports) + toBig(msg.fees.priority_fee_lamports),
      };
      cashLamports = hello.capitalLamports;
      peakEquityLamports = hello.capitalLamports;
      return false;
    }
    if (msg.type === "batch") {
      if (!hello) {
        console.error("batch before hello");
        process.exit(1);
      }
      applyBatch(msg);
      const orders = decideOrders(msg.slot);
      const reply = JSON.stringify({ type: "reply", seq: msg.seq, orders });
      process.stdout.write(reply + "\n");
      return false;
    }
    if (msg.type === "done") {
      return true;
    }
    console.error(`unknown message type: ${String(msg.type)}`);
    return false;
  };

  for await (const chunk of process.stdin) {
    buffer += chunk;
    let idx = buffer.indexOf("\n");
    while (idx !== -1) {
      const line = buffer.slice(0, idx);
      buffer = buffer.slice(idx + 1);
      if (handleLine(line)) {
        done = true;
        break;
      }
      idx = buffer.indexOf("\n");
    }
    if (done) break;
  }
  if (buffer.trim() !== "" && !done) {
    if (handleLine(buffer)) done = true;
  }
  if (!done) {
    console.error("input ended before done");
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err instanceof Error ? err.stack ?? err.message : String(err));
  process.exit(1);
});
