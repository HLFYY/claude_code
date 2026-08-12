// CLI bridge: `node eo_challenge_bridge.js < script.js` -> prints JSON array of every
// `document.cookie = "..."` assignment the script makes, e.g.
//   ["__tst_status=538622729#;", "EO_Bot_Ssid=969736192;"]
//
// This is Tencent EdgeOne's bot-management "cookie challenge": the site returns a page
// whose body is just this heavily obfuscated <script> instead of real content; a real
// browser executes it, which sets one or two cookies and reloads, and the CDN then lets
// the (now cookie-bearing) request through. The obfuscation (array rotation + a numeric
// dispatch table, aka _0x649a-style) and the literal numbers embedded in it change per
// page load, so hand-decoding it once isn't durable -- instead this just runs the real
// script for real in a throwaway Node vm context with document.cookie/location/setTimeout
// stubbed out, and reports what it tried to do. Same approach as tdc_bridge.js.
"use strict";
const vm = require("vm");

let src = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => { src += chunk; });
process.stdin.on("end", () => {
  const cookieWrites = [];
  const sandbox = {
    document: {
      get cookie() { return ""; },
      set cookie(v) { cookieWrites.push(v); },
    },
    location: { href: "" },
    setTimeout: (fn) => { /* don't actually navigate; we only need the cookie writes */ },
    console,
  };
  const context = vm.createContext(sandbox);
  try {
    vm.runInContext(src, context, { filename: "eo_challenge.js", timeout: 5000 });
  } catch (e) {
    console.error("RUN_ERROR: " + (e.stack || e));
    process.exit(1);
  }
  process.stdout.write(JSON.stringify(cookieWrites));
});
