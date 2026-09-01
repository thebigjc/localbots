// One-click simc update, for installs built from source per the README
// (macOS/Linux: git checkout + ninja build, binary symlinked onto PATH).
// Windows nightly binaries can't be rebuilt — detectSimcSource returns null
// there and the UI keeps showing manual instructions instead.
//
// SIMC_REBUILD_CMD escape hatch: when set, it replaces the built-in
// git-pull-then-ninja entirely. Needed whenever the toolchain is not plainly
// on PATH or the build needs post-processing — a Nix/Guix shell, a container,
// a cross-compile, or a PGO/LTO recipe that has to relink and patch the
// binary afterwards. The command is run with cwd=<simc source dir>; any
// non-zero exit is surfaced in the UI exactly like a ninja failure.

import { spawn } from 'node:child_process';
import { realpathSync, existsSync } from 'node:fs';
import { join, dirname } from 'node:path';

// The README recipe puts the binary at <src>/build/simc; resolve the PATH
// symlink and require the build dir (build.ninja) + git checkout (.git).
export function detectSimcSource(simcPath) {
  try {
    const real = realpathSync(simcPath);
    const buildDir = dirname(real);
    const srcDir = dirname(buildDir);
    if (existsSync(join(buildDir, 'build.ninja')) && existsSync(join(srcDir, '.git'))) {
      return { srcDir, buildDir };
    }
  } catch { /* dangling symlink etc. */ }
  return null;
}

// Runs git pull + ninja in the background, reporting into `state`
// ({ running, step, progress, error, log }) and calling onDone(err) once.
export function startSimcUpdate(source, state, onDone) {
  state.running = true;
  state.step = 'pulling the latest simc source';
  state.progress = null;
  state.error = null;
  state.log = [];

  const run = (cmd, args, cwd, onLine) => new Promise((resolve, reject) => {
    const proc = spawn(cmd, args, { cwd, stdio: ['ignore', 'pipe', 'pipe'] });
    let buffer = '';
    const onChunk = (chunk) => {
      buffer += chunk.toString();
      const parts = buffer.split(/[\r\n]/);
      buffer = parts.pop();
      for (const line of parts) {
        if (!line.trim()) continue;
        state.log.push(line);
        if (state.log.length > 30) state.log.shift();
        onLine?.(line);
      }
    };
    proc.stdout.on('data', onChunk);
    proc.stderr.on('data', onChunk);
    proc.on('error', (err) => reject(new Error(`could not run ${cmd}: ${err.message}`)));
    proc.on('close', (code) => {
      if (code === 0) resolve();
      else reject(new Error(`${cmd} failed: ${state.log.slice(-3).join(' · ') || `exit code ${code}`}`));
    });
  });

  const custom = process.env.SIMC_REBUILD_CMD?.trim();

  (async () => {
    const onNinjaLine = (line) => {
      const m = line.match(/^\[(\d+)\/(\d+)\]/);
      if (m) state.progress = { done: Number(m[1]), total: Number(m[2]) };
    };
    if (custom) {
      // The custom command owns the whole update, pull included.
      state.step = 'rebuilding simc (custom recipe)';
      await run('sh', ['-c', custom], source.srcDir, onNinjaLine);
      return;
    }
    await run('git', ['pull', '--ff-only'], source.srcDir);
    state.step = 'building simc (a minute or two)';
    await run('ninja', ['simc'], source.buildDir, onNinjaLine);
  })()
    .then(() => onDone(null))
    .catch((err) => { state.error = err.message; onDone(err); })
    .finally(() => { state.running = false; state.step = null; state.progress = null; });
}
