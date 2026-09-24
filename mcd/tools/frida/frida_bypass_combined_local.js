'use strict';

/*
 * Local-only combined bypass for the ARM64 build of com.mcdonalds.gma.cn.
 *
 * It combines the two working replacements from 44.js with the DexHelper
 * payload analysis:
 *
 *   1. Before libmsaoaidsec.so constructors run, neutralize the two worker
 *      routines used by 44.js (+0x16d30 and +0x175f8).
 *   2. Locate the anonymous DexHelper payload by instruction fingerprints.
 *   3. Change each statically verified "check failed -> intentional low-PC
 *      crash" guard into an unconditional branch to its normal continuation.
 *
 * This intentionally does not NOP the final `br` instruction.  At that point
 * SP/LR have already been zeroed, so NOPing it only changes a deliberate
 * crash into a later invalid return.  The guard before that sequence is the
 * safe patch point.
 *
 * Run this file by itself; do not load 44.js or the older DexHelper scripts
 * at the same time:
 *
 *   frida -U -f com.mcdonalds.gma.cn \
 *     -l /Users/ylm/iaa/com.mcdonalds.gma.cn/frida_bypass_combined_local.js \
 *     --no-pause
 */

const TAG = '[DexHelper-local]';
const MSAO_SO = 'libmsaoaidsec.so';
const MSAO_WORKERS = [0x16d30, 0x175f8];

// Keep this enabled for the current payload. Every entry below was verified
// statically: its fall-through materializes a marker, clears SP/LR, and jumps
// to an unmapped low address; target is the original safe continuation.
const PATCH_ALL_PROVEN_TRAPS = true;
const VERBOSE = false;

// The one check that deliberately kills the parent with direct getppid/kill
// syscalls. It has no zero-SP crash prelude, so it is kept separately.
const PARENT_KILL_GUARD = {
  off: 0x1db6c,
  expected: 0x37300248,
  target: 0x1dbb4,
  label: 'parent SIGKILL guard'
};

// Same intentional-trap form, but encoded as B.cond rather than TBZ/TBNZ.
const EXTRA_GUARDS = [
  { off: 0x12f50, expected: 0x540003e1, target: 0x12fcc,
    label: 'early low-PC trap guard' }
];

// [payload offset, original conditional branch, normal continuation]
const PROVEN_TRAP_GUARDS = [
  [0x1b47c, 0x37082dc8, 0x1ba34],
  [0x1b5e0, 0x370831a8, 0x1bc14],
  [0x1b760, 0x37083348, 0x1bdc8],
  [0x1b8c0, 0x37082b68, 0x1be2c],
  [0x1b9ac, 0x37000788, 0x1ba9c],
  [0x1bd5c, 0x370009a8, 0x1be90],
  [0x1dbbc, 0x373003c8, 0x1dc34],
  [0x1dedc, 0x37f80480, 0x1df6c],
  [0x1dfdc, 0x37f80480, 0x1e06c],
  [0x1f324, 0x37185488, 0x1fdb4],
  [0x230c4, 0x37100448, 0x2314c],
  [0x23438, 0x371006a8, 0x2350c],
  [0x235c8, 0x37180b48, 0x23730],
  [0x23bd4, 0x37180528, 0x23c78],
  [0x300f4, 0x37300448, 0x3017c],
  [0x30360, 0x37305d68, 0x30f0c],
  [0x325c8, 0x370003e8, 0x32644],
  [0x36c64, 0x37300828, 0x36d68],
  [0x36cec, 0x37000668, 0x36db8],
  [0x49c50, 0x37001d08, 0x49ff0],
  [0x49f74, 0x370003e8, 0x49ff0],
  [0x4a934, 0x37000888, 0x4aa44],
  [0x4a9cc, 0x37000ba8, 0x4ab40],
  [0x4aab8, 0x37000ca8, 0x4ac4c],
  [0x4b49c, 0x370010c8, 0x4b6b4],
  [0x4bb1c, 0x37000368, 0x4bb88],
  [0x4d524, 0x370003e8, 0x4d5a0],
  [0x4d610, 0x37001548, 0x4d8b8],
  [0x4d698, 0x370003c8, 0x4d710],
  [0x4d9d8, 0x37000608, 0x4da98],
  [0x4db4c, 0x370003c8, 0x4dbc4],
  [0x4dc60, 0x37000448, 0x4dce8],
  [0x4de6c, 0x37385288, 0x4e8bc],
  [0x4e0d0, 0x373839c8, 0x4e808],
  [0x4e78c, 0x373806a8, 0x4e860],
  [0x4ed70, 0x37180448, 0x4edf8],
  [0x4f4bc, 0x37180368, 0x4f528],
  [0x4f758, 0x37100828, 0x4f85c],
  [0x4f7e0, 0x37104908, 0x50100],
  [0x502d4, 0x371003c8, 0x5034c],
  [0x509c0, 0x371003c8, 0x50a38],
  [0x50b9c, 0x371003c8, 0x50c14],
  [0x51a54, 0x37080448, 0x51adc],
  [0x526e0, 0x37080668, 0x527ac],
  [0x53d08, 0x37000529, 0x53dac],
  [0x53e5c, 0x37380509, 0x53efc],
  [0x53fc0, 0x37000be9, 0x5413c],
  [0x5403c, 0x37080449, 0x540c4],
  [0x58400, 0x37001aa8, 0x58754],
  [0x58a10, 0x37007848, 0x59918],
  [0x58cf8, 0x370003c8, 0x58d70],
  [0x5989c, 0x37000628, 0x59960],
  [0x59d8c, 0x370003e8, 0x59e08],
  [0x5ec48, 0x371813e8, 0x5eec4],
  [0x5f1f4, 0x371803c8, 0x5f26c],
  [0x5f314, 0x371803e8, 0x5f390],
  [0x60010, 0x371003e8, 0x6008c],
  [0x60e58, 0x372803e8, 0x60ed4],
  [0x62144, 0x37280ae8, 0x622a0],
  [0x63b00, 0x37200a88, 0x63c50],
  [0x63b74, 0x37200448, 0x63bfc],
  [0x64514, 0x372803e8, 0x64590],
  [0x6482c, 0x373803e8, 0x648a8],
  [0x66e90, 0x371003e8, 0x66f0c],
  [0x67398, 0x370803e8, 0x67414],
  [0x67748, 0x37203828, 0x67e4c],
  [0x67dc4, 0x372006c8, 0x67e9c],
  [0x6a450, 0x373003e8, 0x6a4cc],
  [0x6ab98, 0x370003e8, 0x6ac14],
  [0x6ae4c, 0x37080b08, 0x6afac],
  [0x6b2bc, 0x371803e8, 0x6b338],
  [0x6c6c4, 0x372003e8, 0x6c740],
  [0x6c8b4, 0x37286a28, 0x6d5f8],
  [0x6d768, 0x372803e8, 0x6d7e4],
  [0x712e4, 0x37104528, 0x71b88],
  [0x71ab4, 0x37100448, 0x71b3c],
  [0x71de8, 0x37100368, 0x71e54],
  [0x73afc, 0x371003e8, 0x73b78],
  [0x74044, 0x37101048, 0x7424c],
  [0x7443c, 0x37102428, 0x748c0],
  [0x74848, 0x37100fc8, 0x74a40],
  [0x749c8, 0x371006a8, 0x74a9c],
  [0x74b9c, 0x371003e8, 0x74c18]
];

const FINGERPRINT = [
  [0x1dbc0, 0x5290f3ab], [0x1dbc4, 0x52903e6c],
  [0x1dbc8, 0x5280f38a], [0x1dbcc, 0x72b6d44b],
  [0x4e0d4, 0x529111cb], [0x4e0d8, 0x529111ac],
  [0x4e0dc, 0x5281118a], [0x4e0e0, 0x72b6d44b]
];

const MIN_PAYLOAD_RANGE = 0x4e200;
const MAX_PAYLOAD_RANGE = 0x800000;
const PAYLOAD_POLL_MS = 25;

let payloadPatched = false;
let payloadBusy = false;
let payloadTimer = null;
let msaoPatched = false;
let msaoLoadHooked = false;
let linkerHooked = false;
let msaoCallbacks = [];
let workerHits = Object.create(null);

function log(message) {
  console.log(TAG + ' ' + message);
}

function hex32(value) {
  const text = (value >>> 0).toString(16);
  return '0x' + ('00000000' + text).slice(-8);
}

function readU32(address) {
  try {
    return address.readU32() >>> 0;
  } catch (_) {
    return null;
  }
}

function findModule(name) {
  try {
    return Process.findModuleByName(name);
  } catch (_) {
    return null;
  }
}

function findExport(name) {
  try {
    const address = Module.findExportByName(null, name);
    if (address !== null) return address;
  } catch (_) {}

  try {
    if (typeof Module.findGlobalExportByName === 'function') {
      const address = Module.findGlobalExportByName(name);
      if (address !== null) return address;
    }
  } catch (_) {}

  return null;
}

function installMsaoGuards(source) {
  if (msaoPatched) return true;

  const module = findModule(MSAO_SO);
  if (module === null) return false;

  let installed = 0;
  for (const offset of MSAO_WORKERS) {
    const address = module.base.add(offset);
    try {
      const callback = new NativeCallback(function (_) {
        const key = offset.toString(16);
        if (workerHits[key] === undefined) {
          workerHits[key] = 1;
          log('blocked ' + MSAO_SO + '+0x' + key);
        }
        // These targets are used as pthread-style workers. Return NULL rather
        // than leaving x0 undefined, which is safer than the void callback in
        // the original 44.js.
        return ptr(0);
      }, 'pointer', ['pointer']);

      Interceptor.replace(address, callback);
      msaoCallbacks.push(callback); // Keep NativeCallbacks alive.
      installed++;
    } catch (error) {
      console.error(TAG + ' could not replace ' + MSAO_SO + '+0x' +
        offset.toString(16) + ': ' + error);
    }
  }

  msaoPatched = installed === MSAO_WORKERS.length;
  if (msaoPatched) {
    log('44.js worker bypass installed via ' + source);
  }
  return msaoPatched;
}

function hookMsaoLoads() {
  if (msaoLoadHooked) return;
  const loader = findExport('android_dlopen_ext');
  if (loader === null) {
    log('android_dlopen_ext unavailable; using linker/poll fallback');
    return;
  }

  try {
    Interceptor.attach(loader, {
      onEnter: function (args) {
        this.target = false;
        try {
          this.target = !args[0].isNull() &&
            args[0].readCString().indexOf(MSAO_SO) !== -1;
        } catch (_) {}
      },
      onLeave: function () {
        if (this.target) {
          setImmediate(function () {
            installMsaoGuards('android_dlopen_ext fallback');
          });
        }
      }
    });
    msaoLoadHooked = true;
  } catch (error) {
    console.error(TAG + ' android_dlopen_ext hook failed: ' + error);
  }
}

function hookLinkerConstructors() {
  if (linkerHooked) return;

  const linker = findModule(Process.pointerSize === 8 ? 'linker64' : 'linker');
  if (linker === null) return;

  let constructors = null;
  try {
    const symbols = linker.enumerateSymbols();
    for (const symbol of symbols) {
      if (symbol.name.indexOf('call_constructors') !== -1) {
        constructors = symbol.address;
        break;
      }
    }
  } catch (_) {}

  if (constructors === null) {
    log('linker call_constructors symbol unavailable; using dlopen fallback');
    return;
  }

  try {
    Interceptor.attach(constructors, {
      onEnter: function () {
        // This is the timing used by 44.js: the target module is mapped, but
        // its constructor chain has not run yet.
        installMsaoGuards('linker call_constructors');
      }
    });
    linkerHooked = true;
    log('hooked linker call_constructors @ ' + constructors);
  } catch (error) {
    console.error(TAG + ' linker hook failed: ' + error);
  }
}

function makeBranch(fromOffset, targetOffset) {
  const delta = targetOffset - fromOffset;
  if ((delta & 3) !== 0 || delta < -0x8000000 || delta >= 0x8000000) {
    throw new Error('invalid ARM64 B target +' + targetOffset.toString(16));
  }
  return (0x14000000 | ((delta / 4) & 0x03ffffff)) >>> 0;
}

function patchBranch(base, item) {
  const address = base.add(item.off);
  const replacement = makeBranch(item.off, item.target);
  const before = readU32(address);

  if (before === replacement) return 'already';
  if (before !== item.expected) {
    if (VERBOSE) {
      log('skip +0x' + item.off.toString(16) + ': expected ' +
        hex32(item.expected) + ', got ' +
        (before === null ? '<unreadable>' : hex32(before)));
    }
    return 'mismatch';
  }

  Memory.patchCode(address, 4, function (code) {
    code.writeU32(replacement);
  });

  if (readU32(address) !== replacement) {
    throw new Error('write verification failed at ' + address);
  }
  return 'patched';
}

function payloadMatches(base) {
  for (const pair of FINGERPRINT) {
    if (readU32(base.add(pair[0])) !== pair[1]) return false;
  }

  const parent = readU32(base.add(PARENT_KILL_GUARD.off));
  const parentB = makeBranch(PARENT_KILL_GUARD.off, PARENT_KILL_GUARD.target);
  if (parent !== PARENT_KILL_GUARD.expected && parent !== parentB) return false;

  // These are the two tombstone paths that originally hit 0x79c and 0x88c.
  for (const index of [6, 33]) {
    const entry = PROVEN_TRAP_GUARDS[index];
    const current = readU32(base.add(entry[0]));
    const branch = makeBranch(entry[0], entry[2]);
    if (current !== entry[1] && current !== branch) return false;
  }
  return true;
}

function patchPayload(base) {
  if (payloadPatched || payloadBusy || !payloadMatches(base)) return false;

  payloadBusy = true;
  try {
    const entries = [PARENT_KILL_GUARD].concat(EXTRA_GUARDS);
    if (PATCH_ALL_PROVEN_TRAPS) {
      for (const item of PROVEN_TRAP_GUARDS) {
        entries.push({ off: item[0], expected: item[1], target: item[2],
          label: 'proven low-PC trap guard' });
      }
    }

    let patched = 0;
    let already = 0;
    let mismatch = 0;
    for (const entry of entries) {
      const result = patchBranch(base, entry);
      if (result === 'patched') patched++;
      else if (result === 'already') already++;
      else mismatch++;
    }

    payloadPatched = true;
    if (payloadTimer !== null) {
      clearInterval(payloadTimer);
      payloadTimer = null;
    }

    log('payload=' + base + ', patched=' + patched +
      ', already=' + already + ', unexpected=' + mismatch);
    if (mismatch !== 0) {
      console.error(TAG + ' payload layout partly differs; enable VERBOSE to list offsets');
    }
    return true;
  } catch (error) {
    console.error(TAG + ' payload patch failed: ' + error.stack);
    return false;
  } finally {
    payloadBusy = false;
  }
}

function enumerateRanges(protection) {
  try {
    return Process.enumerateRanges({ protection: protection, coalesce: false });
  } catch (_) {
    return Process.enumerateRanges(protection);
  }
}

function probePayload() {
  if (payloadPatched || payloadBusy) return;

  payloadBusy = true;
  try {
    const seen = Object.create(null);
    for (const protection of ['rwx', 'r-x']) {
      let ranges;
      try {
        ranges = enumerateRanges(protection);
      } catch (_) {
        continue;
      }
      for (const range of ranges) {
        if (range.size < MIN_PAYLOAD_RANGE || range.size > MAX_PAYLOAD_RANGE) {
          continue;
        }
        const key = range.base.toString();
        if (seen[key]) continue;
        seen[key] = true;

        // Avoid nested busy-state rejection while retaining one scan pass.
        if (payloadMatches(range.base)) {
          payloadBusy = false;
          patchPayload(range.base);
          return;
        }
      }
    }
  } finally {
    payloadBusy = false;
  }
}

function installResidualExceptionLog() {
  Process.setExceptionHandler(function (details) {
    const context = details.context || {};
    console.error(TAG + ' residual exception: ' + details.type +
      ' pc=' + context.pc + ' x8=' + context.x8 +
      ' x9=' + context.x9 + ' sp=' + context.sp);
    // Observe only. Do not resume an unknown crash site.
    return false;
  });
}

function main() {
  if (Process.arch !== 'arm64') {
    console.error(TAG + ' ARM64 only; current arch=' + Process.arch);
    return;
  }

  installResidualExceptionLog();
  hookMsaoLoads();
  hookLinkerConstructors();
  installMsaoGuards('already loaded');

  probePayload();
  if (!payloadPatched) {
    payloadTimer = setInterval(probePayload, PAYLOAD_POLL_MS);
    log('waiting for anonymous DexHelper payload');
  }
}

// Run synchronously while the process is still suspended by `frida -f`.
main();
