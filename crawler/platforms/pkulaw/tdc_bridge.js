// CLI bridge: `node tdc_bridge.js <path-to-tdc.js>` -> prints JSON
// {"collect": TDC.getData(true), "eks": TDC.getInfo().info, "tokenid": ...} to stdout.
//
// This runs tdc.js's OWN code for real inside a plain Node vm context with hand-written
// stub globals (window/document/navigator/screen/location/canvas-2d/localStorage/...).
// It is NOT a real browser and NOT Puppeteer/Selenium/JSDOM -- just enough surface area
// for tdc.js to execute without throwing. We do not reimplement its internal algorithm.
"use strict";
const fs = require("fs");
const vm = require("vm");

const tdcPath = process.argv[2];
if (!tdcPath) {
  console.error("usage: node tdc_bridge.js <path-to-tdc.js>");
  process.exit(1);
}
const src = fs.readFileSync(tdcPath, "utf8");

function buildSandbox() {
  const fakeStorage = () => {
    const store = {};
    return {
      getItem: (k) => (k in store ? store[k] : null),
      setItem: (k, v) => { store[k] = String(v); },
      removeItem: (k) => { delete store[k]; },
      clear: () => { for (const k in store) delete store[k]; },
      key: (i) => Object.keys(store)[i] || null,
      get length() { return Object.keys(store).length; },
    };
  };

  const window = {};
  const document = {
    createElement: (tag) => ({
      tagName: String(tag).toUpperCase(), style: {}, attributes: {}, childNodes: [],
      addEventListener() {}, removeEventListener() {},
      setAttribute(k, v) { this.attributes[k] = v; }, getAttribute(k) { return this.attributes[k]; },
      appendChild(c) { this.childNodes.push(c); return c; },
      getContext(type) {
        if (type === "2d") {
          return {
            fillRect() {}, fillText() {}, strokeText() {}, measureText: () => ({ width: 0 }),
            getImageData: () => ({ data: new Uint8ClampedArray(4) }),
            putImageData() {}, drawImage() {}, save() {}, restore() {}, translate() {}, scale() {}, rotate() {},
            beginPath() {}, closePath() {}, arc() {}, fill() {}, stroke() {},
            createLinearGradient: () => ({ addColorStop() {} }),
            set fillStyle(v) {}, set strokeStyle(v) {}, set font(v) {}, set textBaseline(v) {}, set globalAlpha(v) {},
          };
        }
        return null;
      },
      toDataURL: () => "data:image/png;base64,",
      width: 300, height: 150,
    }),
    createElementNS: (ns, tag) => document.createElement(tag),
    getElementsByTagName: () => [], querySelector: () => null, querySelectorAll: () => [],
    addEventListener() {}, removeEventListener() {},
    documentElement: { style: {}, clientWidth: 1512, clientHeight: 982 },
    body: { appendChild() {}, style: {}, addEventListener() {}, removeEventListener() {} },
    cookie: "", referrer: "https://cas.pkulaw.com/", visibilityState: "visible", hidden: false,
    readyState: "complete", location: null, title: "",
  };

  const navigator = {
    userAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    platform: "MacIntel", language: "zh-CN", languages: ["zh-CN", "zh", "en"],
    hardwareConcurrency: 8, deviceMemory: 8, maxTouchPoints: 0, plugins: [], mimeTypes: [],
    vendor: "Google Inc.", appVersion: "5.0 (Macintosh)", webdriver: false, cookieEnabled: true,
    doNotTrack: null, onLine: true, sendBeacon: () => true,
  };

  const screen = { width: 1512, height: 982, availWidth: 1512, availHeight: 950, colorDepth: 30, pixelDepth: 30 };
  const location = {
    href: "https://turing.captcha.gtimg.com/1/template/drag_ele.html",
    protocol: "https:", host: "turing.captcha.gtimg.com", hostname: "turing.captcha.gtimg.com",
    pathname: "/1/template/drag_ele.html", search: "", hash: "", origin: "https://turing.captcha.gtimg.com",
  };
  document.location = location;

  Object.assign(window, {
    window, self: window, top: window, parent: window,
    document, navigator, screen, location,
    localStorage: fakeStorage(), sessionStorage: fakeStorage(),
    console, setTimeout, clearTimeout, setInterval, clearInterval,
    Date, Math, JSON, Array, Object, String, Number, Boolean, RegExp, Error, Promise,
    Uint8Array, Uint8ClampedArray, Float32Array, ArrayBuffer, DataView,
    addEventListener() {}, removeEventListener() {}, postMessage() {},
    performance: { now: () => Date.now(), timing: {}, getEntriesByType: () => [] },
    innerWidth: 1512, innerHeight: 982, outerWidth: 1512, outerHeight: 982, devicePixelRatio: 2,
    requestAnimationFrame: (fn) => setTimeout(fn, 16), cancelAnimationFrame: (id) => clearTimeout(id),
    crypto: { getRandomValues: (arr) => { for (let i = 0; i < arr.length; i++) arr[i] = Math.floor(Math.random() * 256); return arr; } },
  });
  return window;
}

const window = buildSandbox();
const context = vm.createContext(window);

try {
  vm.runInContext(src, context, { filename: "tdc.js", timeout: 5000 });
} catch (e) {
  console.error("LOAD_ERROR: " + (e.stack || e));
  process.exit(2);
}

if (!context.TDC || typeof context.TDC.getData !== "function" || typeof context.TDC.getInfo !== "function") {
  console.error("TDC_NOT_READY: window.TDC did not expose getData/getInfo");
  process.exit(3);
}

try {
  const collect = context.TDC.getData(true);
  const info = context.TDC.getInfo() || {};
  process.stdout.write(JSON.stringify({ collect, eks: info.info || "", tokenid: info.tokenid }));
} catch (e) {
  console.error("CALL_ERROR: " + (e.stack || e));
  process.exit(4);
}
