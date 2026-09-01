// Distribute one profileset sim across several machines with GNU parallel.
//
// A droptimizer/top-gear input is a base profile plus N independent
// `profileset."name"=...` lines, and simc reports each as its own row. That
// makes it embarrassingly parallel at the profileset level: split the lines
// into chunks, sim each chunk anywhere, concatenate the result arrays. No
// scheduler, no coordination, no shared state.
//
// Enabled by SIMC_HOSTS, using GNU parallel's sshlogin syntax:
//   SIMC_HOSTS="4/:,10/mac-ci"      ':' is this machine
//
// Slots should be PHYSICAL cores, not nproc: simc gets only ~2-3% from SMT,
// so counting logical CPUs makes an SMT host claim ~2x its fair share and
// become the straggler everything waits on. Each chunk runs threads=1 --
// N independent single-threaded sims beat one N-threaded sim, which scales
// only ~3.2x on 7 threads.

import { spawn } from 'node:child_process';
import { writeFileSync, readFileSync, existsSync, mkdirSync } from 'node:fs';
import { join, basename } from 'node:path';

const PROFILESET_RE = /^profileset\."([^"]+)"\s*\+?=/;

// simc's own progress line, as it appears once --tag has prefixed the chunk:
//   chunk-003.simc\tGenerating Profileset: feet+2 3/8 [==>...] 4200/10000 267.4 1sec
// Also matches the baseline phase, which has no item name.
const TAGGED_RE = /^(\S+)\t(?:Generating (Profileset|Baseline)?:?\s*)?(.*?)\s*(\d+)\/(\d+)\s*\[[^\]]*\]\s*(\d+)\/(\d+)\s*([\d.]+)?/;

function parseTagged(line) {
  const m = line.match(TAGGED_RE);
  if (!m) return null;
  const [, chunk, phase, item, phaseNum, phaseTotal, iterDone, iterTotal, mean] = m;
  // simc emits "Generating Baseline:" with no name, so keep the phase word --
  // without it a baseline row has nothing to show but its chunk filename.
  return { chunk, phase: phase || 'Profileset',
           item: (item || '').replace(/^Profileset:?\s*/, '').trim() || null,
           phaseNum: +phaseNum, phaseTotal: +phaseTotal,
           iterDone: +iterDone, iterTotal: +iterTotal,
           meanDps: mean ? +mean : null };
}

// Split input into the shared base profile and per-profileset line groups.
// A profileset spans multiple lines ("name"= then "name"+=), so lines are
// grouped by name and never split apart.
export function parseInput(inputText) {
  const base = [];
  const groups = new Map(); // name -> lines[]
  for (const line of inputText.split('\n')) {
    const m = line.match(PROFILESET_RE);
    if (m) {
      if (!groups.has(m[1])) groups.set(m[1], []);
      groups.get(m[1]).push(line);
    } else {
      base.push(line);
    }
  }
  return { base: base.join('\n'), groups };
}

// Round-robin rather than contiguous: profileset cost varies (a trinket with
// a proc chain sims slower than a stat stick), and round-robin spreads that
// unevenness across chunks instead of concentrating it in one.
// When `iterations` is known we can stop paying for the baseline K times.
// simc sims the base profile once per process before running that process's
// profilesets, so K chunks means K baselines -- pure waste for K-1 of them.
// A profileset may override sim-scope `iterations`, so: pin the real count
// onto every profileset, and let the base run a single throwaway iteration in
// every chunk but the first. Chunk 0 keeps a full baseline, which is the one
// localbots reports as "your current DPS" and ranks against.
//
// Caveat: chunks 1..K-1 then have a meaningless sim.players[0] DPS, so
// mergeResults cannot use them to cancel per-chunk bias. That only matters on
// a mixed-architecture fleet; pass sharedBaseline:false there to keep a real
// baseline in every chunk.
export function buildChunks(inputText, chunkCount, { iterations = null, sharedBaseline = true } = {}) {
  const { base, groups } = parseInput(inputText);
  const names = [...groups.keys()];
  if (names.length === 0) return { base, chunks: [] };
  const k = Math.max(1, Math.min(chunkCount, names.length));
  const buckets = Array.from({ length: k }, () => []);
  const pin = iterations && sharedBaseline;
  names.forEach((n, i) => {
    const lines = groups.get(n).slice();
    if (pin) lines.push(`profileset."${n}"+=iterations=${iterations}`);
    buckets[i % k].push(...lines);
  });
  const chunks = buckets.filter((b) => b.length).map((b, i) => {
    const head = pin ? `${base}\niterations=${i === 0 ? iterations : 1}\n` : `${base}\n`;
    return `${head}${b.join('\n')}\n`;
  });
  return { base, chunks, pinned: pin };
}

// Total slots across all hosts, e.g. "4/:,10/mac-ci" -> 14.
export function totalSlots(hosts) {
  return hosts.split(',').reduce((sum, h) => {
    const m = h.trim().match(/^(\d+)\//);
    return sum + (m ? Number(m[1]) : 1);
  }, 0);
}

// Concatenate the per-chunk result arrays into one simc-shaped report.
//
// Each chunk sims its own copy of the baseline, so every chunk's numbers sit
// on their own RNG (and, across architectures, its own floating-point)
// footing. Rescaling each chunk's profileset means by chunk0Baseline/ownBaseline
// puts them all on a common scale, so per-chunk bias cancels instead of
// leaking into the item ranking. With one machine and a fixed seed the
// baselines are identical and this is a no-op.
export function mergeResults(jsons) {
  if (jsons.length === 0) throw new Error('no chunk results to merge');
  const baseOf = (j) => j?.sim?.players?.[0]?.collected_data?.dps?.mean ?? 0;
  const merged = jsons[0];
  const ref = baseOf(merged);
  const results = [];
  const drift = [];
  // A chunk whose baseline ran a single throwaway iteration has a meaningless
  // mean and a huge error bar; rescaling against it would inject noise rather
  // than remove it. Detect and skip those.
  const iterOf = (j) => j?.sim?.players?.[0]?.collected_data?.dps?.count ?? 0;
  const refIters = iterOf(merged);
  for (const j of jsons) {
    const own = baseOf(j);
    const trustworthy = iterOf(j) >= Math.max(2, refIters / 10);
    const scale = trustworthy && ref > 0 && own > 0 ? ref / own : 1;
    if (scale !== 1) drift.push(Math.abs(1 - scale) * 100);
    for (const r of j?.sim?.profilesets?.results ?? []) {
      results.push(scale === 1 ? r : { ...r, mean: r.mean * scale });
    }
  }
  if (!merged.sim.profilesets) merged.sim.profilesets = {};
  merged.sim.profilesets.results = results;
  return { merged, baselineDrift: drift.length ? Math.max(...drift) : 0 };
}

// Verify every chunk came from the same simc source revision. Mismatched game
// data produces plausible-looking but wrong rankings with no error anywhere,
// so this is checked on the way back, not just before dispatch.
export function assertSameBuild(jsons) {
  const key = (j) => `${j.version}/${j.git_revision}/${j.ptr_enabled}`;
  const seen = new Map();
  for (const j of jsons) {
    const k = key(j);
    seen.set(k, (seen.get(k) ?? 0) + 1);
  }
  if (seen.size > 1) {
    throw new Error(
      `workers disagree on simc build (game data would differ): ${[...seen.keys()].join(' vs ')}`
    );
  }
}

// Names of every profileset defined in a chunk file, longest first. Matching
// longest-first matters: a regex capture would truncate at the first quote for
// item names that themselves contain one (localbots hits the same case).
function chunkProfilesetNames(text) {
  const names = new Set();
  for (const line of text.split('\n')) {
    const m = line.match(/^profileset\."([^"]+)"/);
    if (m) names.add(m[1]);
  }
  return [...names].sort((a, b) => b.length - a.length);
}

// Drop one profileset from a chunk, returning the new text.
function dropProfileset(text, name) {
  return text
    .split('\n')
    .filter((l) => !l.startsWith(`profileset."${name}"`))
    .join('\n');
}

// Run the whole thing. Writes the merged report to jsonPath and resolves with
// stats. onLine gets progress text; the returned handle exposes kill().
//
// Chunks that die because simc cannot initialise one item are healed rather
// than fatal: the offending profileset is dropped from that chunk and only
// that chunk re-runs. This mirrors the self-heal simRunner already does for
// local sims -- without it a single unloadable item loses the entire run,
// which is exactly what "parallel exited 82" was.
export function runDistributed({ inputText, jsonPath, dir, hosts, simcPath, seed = 12345, iterations = null, sharedBaseline = true, oversubscribe = 3, maxHeals = 5, onLine = () => {}, onProgress = () => {}, onDone }) {
  const slots = totalSlots(hosts);
  const { chunks, pinned } = buildChunks(inputText, slots * oversubscribe, { iterations, sharedBaseline });
  if (chunks.length === 0) return { error: 'no profilesets to distribute' };

  const chunkDir = join(dir, 'chunks');
  if (!existsSync(chunkDir)) mkdirSync(chunkDir, { recursive: true });
  const files = chunks.map((text, i) => {
    const f = join(chunkDir, `chunk-${String(i).padStart(3, '0')}.simc`);
    writeFileSync(f, text);
    return f;
  });
  // exact profileset count per chunk, so overall progress is counted rather
  // than extrapolated from whichever workers happen to be reporting
  const chunkSets = new Map(files.map((f, i) => [basename(f), chunkProfilesetNames(chunks[i]).length]));
  const setsTotal = [...chunkSets.values()].reduce((a, b) => a + b, 0);

  const remote = hosts.split(',')
    .map((h) => h.trim().replace(/^\d+\//, ''))
    .some((h) => h !== ':' && h !== '');
  const bin = remote ? (process.env.SIMC_REMOTE_BIN || '/home/jc/bin/simc') : simcPath;

  let cancelled = false;
  let current = null;
  const dropped = [];
  const total = files.length;
  const doneCount = () => files.filter((f) => existsSync(`${f}.json`)).length;
  // live state per chunk, keyed by basename, so the UI can show what each
  // worker is actually doing rather than just how many chunks have landed
  const live = new Map();

  const runRound = (todo) => new Promise((resolve) => {
    const args = ['--sshlogin', hosts, '--line-buffer', '--tag', '--tagstring', '{/}',
                  '--joblog', join(chunkDir, `joblog-${todo.length}-${Date.now()}`)];
    if (remote) args.push('--transferfile', '{}', '--return', '{}.json', '--return', '{}.log', '--cleanup', '--workdir', '...');
    // tee, not redirect: the log is needed to diagnose and heal a failed chunk,
    // but sending stdout there too leaves parallel's stream empty and the UI
    // with nothing to report but "N of M chunks". --tag prefixes every line
    // with its chunk file so progress can be attributed to a worker.
    let cmd = `${bin} {} json2={}.json threads=1 report_details=1 seed=${seed}`;
    if (iterations && !pinned) cmd += ` iterations=${iterations}`;
    cmd += ` 2>&1 | tee {}.log`;
    args.push(cmd, ':::', ...todo);

    const proc = spawn('parallel', args, { stdio: ['ignore', 'pipe', 'pipe'] });
    current = proc;
    let buf = '';
    const onData = (d) => {
      buf += d.toString();
      const parts = buf.split(/[\r\n]/); buf = parts.pop();
      for (const l of parts) {
        if (!l.trim()) continue;
        const p = parseTagged(l);
        if (p) live.set(p.chunk, { ...p, at: Date.now() });
        else onLine(l);
      }
    };
    proc.stdout.on('data', onData);
    proc.stderr.on('data', onData);
    proc.on('error', (e) => resolve({ error: new Error(`could not run parallel: ${e.message}`) }));
    proc.on('close', () => resolve({}));
  });

  const timer = setInterval(() => {
    const done = doneCount();
    // A finished chunk keeps reporting its last line forever, which would both
    // inflate the worker count past the slot count and double-count its
    // profilesets. Drop anything whose result has landed.
    let finishedSets = 0;
    for (const f of files) {
      const b = basename(f);
      if (existsSync(`${f}.json`)) { live.delete(b); finishedSets += chunkSets.get(b) ?? 0; }
    }
    const workers = [...live.values()].sort((a, b) => a.chunk.localeCompare(b.chunk));
    // Completed chunks contribute their whole size; a running chunk contributes
    // the profilesets it has finished, phaseNum being 1-based on the current one.
    const setsDone = Math.min(setsTotal,
      finishedSets + workers.reduce((n, w) => n + Math.max(0, w.phaseNum - 1), 0));
    onProgress({ done, total, workers, setsDone, setsTotal });
  }, 1000);

  (async () => {
    let todo = files.slice();
    for (let round = 0; round <= maxHeals && todo.length && !cancelled; round++) {
      const r = await runRound(todo);
      if (cancelled) break;   // falls through to the notify below
      if (r.error) { clearInterval(timer); return onDone(r.error); }

      const failed = todo.filter((f) => !existsSync(`${f}.json`));
      if (failed.length === 0) { todo = []; break; }
      if (round === maxHeals) break;

      // Heal what we can: find the profileset simc rejected and drop just it.
      const next = [];
      for (const f of failed) {
        const log = existsSync(`${f}.log`) ? readFileSync(`${f}.log`, 'utf8') : '';
        const text = readFileSync(f, 'utf8');
        const bad = chunkProfilesetNames(text).find((n) => log.includes(`Profileset '${n}'`));
        if (!bad) continue; // not a droppable profileset error -- leave it failed
        writeFileSync(f, dropProfileset(text, bad));
        dropped.push(bad);
        onLine(`dropped incompatible profileset "${bad}", retrying its chunk`);
        next.push(f);
      }
      if (next.length === 0) break; // nothing healable; stop retrying
      todo = next;
    }
    clearInterval(timer);
    // The caller has to be told, or the job sits in 'running' forever and the
    // cancel button looks dead. The local path gets this free from
    // proc.on('close') firing after SIGKILL; a fan-out has no such event.
    if (cancelled) return onDone(new Error('cancelled'));

    try {
      const ok = files.filter((f) => existsSync(`${f}.json`));
      if (ok.length === 0) {
        const anyLog = files.map((f) => (existsSync(`${f}.log`) ? readFileSync(`${f}.log`, 'utf8') : ''))
          .find((t) => /^Error:/m.test(t)) ?? '';
        const first = anyLog.split('\n').find((l) => l.startsWith('Error:'));
        throw new Error(first || 'every chunk failed');
      }
      const jsons = ok.map((f) => JSON.parse(readFileSync(`${f}.json`, 'utf8')));
      assertSameBuild(jsons);
      const { merged, baselineDrift } = mergeResults(jsons);
      writeFileSync(jsonPath, JSON.stringify(merged));
      const n = merged.sim.profilesets.results.length;
      onLine(`merged ${n} profileset results from ${jsons.length}/${total} chunks` +
             (dropped.length ? `, dropped ${dropped.length} incompatible` : '') +
             (baselineDrift ? ` (max baseline drift ${baselineDrift.toFixed(3)}%)` : ''));
      onDone(null, { chunks: jsons.length, total, results: n, dropped, baselineDrift });
    } catch (e) {
      onDone(e);
    }
  })();

  return {
    kill: () => {
      cancelled = true;
      clearInterval(timer);
      current?.kill('SIGKILL');
      for (const h of hosts.split(',').map((x) => x.trim().replace(/^\d+\//, ''))) {
        if (h === ':' || !h) continue;
        try {
          spawn('ssh', ['-o', 'BatchMode=yes', h, `pkill -f '${chunkDir}' || true`], { stdio: 'ignore' }).unref();
        } catch { /* best effort */ }
      }
    },
  };
}
