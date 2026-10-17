/*
 * hook_fake_device.js —— 路线A PoC:在采集源注入假设备,让 app 自己加密→注册新 smid
 * 注入点:
 *   - Java: Settings.Secure.getString(android_id) → 假值
 *   - native: __system_property_get(ro.serialno/fingerprint/product.* …) → 假值
 *   - 清 smid 缓存: hook 读本地 smid 的方法返回 ""，强制走 vdata 重新注册
 * rpc.exports: getd()/getvdata()
 * 建议 spawn 早期注入(fake_device_poc.py)
 *
 * 假设备 Profile(自洽: 小米 Redmi K40 / alioth)。换设备改 PROFILE 即可。
 */
var PROFILE = (typeof INJECT_PROFILE !== 'undefined') ? INJECT_PROFILE : {
    android_id: "facefacefaceface",          // 16 hex 假 AndroidID(默认;device_pool.py 会注入 INJECT_PROFILE 覆盖)
    token: "facefacefacefacefacefaceface0000",  // 设备固定 token(等价真机持久化 UUID;INJECT_PROFILE 会覆盖)
    props: {
        "ro.serialno": "FAKE1A2B3C4D",
        "ro.boot.serialno": "FAKE1A2B3C4D",
        "ro.product.model": "M2012K11AC",
        "ro.product.brand": "Redmi",
        "ro.product.manufacturer": "Xiaomi",
        "ro.product.device": "alioth",
        "ro.product.name": "alioth",
        "ro.build.fingerprint": "Redmi/alioth/alioth:11/RKQ1.200826.002/V12.5.1.0.RKHCNXM:user/release-keys",
        "ro.hardware": "qcom",
        "ro.boot.hardware": "qcom",
        "ro.boot.bootloader": "unknown"
    }
};
// 覆盖各分区变体(数美可能读 vendor/system/odm 版本)
(function expand() {
    var parts = ["system", "vendor", "odm", "product", "system_ext"];
    var base = {};
    Object.keys(PROFILE.props).forEach(function (k) { base[k] = PROFILE.props[k]; });
    Object.keys(base).forEach(function (k) {
        var m = k.match(/^ro\.product\.(.+)$/);
        if (m) parts.forEach(function (p) { PROFILE.props["ro.product." + p + "." + m[1]] = base[k]; });
        var f = k.match(/^ro\.build\.(.+)$/);
        if (f) parts.forEach(function (p) { PROFILE.props["ro." + p + ".build." + f[1]] = base[k]; });
    });
})();

function onClassReady(name, fn, n) {
    n = (n === undefined) ? 40 : n;
    try { fn(Java.use(name)); console.log("[fake] hooked " + name); }
    catch (e) {
        if (n > 0) setTimeout(function () { Java.perform(function () { onClassReady(name, fn, n - 1); }); }, 400);
    }
}

// ---------- native: __system_property_get ----------
(function hookProp() {
    var spg = Module.findExportByName("libc.so", "__system_property_get");
    if (!spg) { console.log("[fake][!] 找不到 __system_property_get"); return; }
    Interceptor.attach(spg, {
        onEnter: function (a) { this.name = a[0].readCString(); this.vbuf = a[1]; },
        onLeave: function (ret) {
            var v = PROFILE.props[this.name];
            if (v !== undefined && this.vbuf && !this.vbuf.isNull()) {
                if (v.length > 91) v = v.substring(0, 91);   // PROP_VALUE_MAX=92
                this.vbuf.writeUtf8String(v);
                ret.replace(ptr(v.length));
            }
        }
    });
    console.log("[fake] native __system_property_get hooked");
})();

Java.perform(function () {
    // ---------- Java: Settings.Secure.getString(android_id) ----------
    onClassReady("android.provider.Settings$Secure", function (S) {
        S.getString.overload("android.content.ContentResolver", "java.lang.String").implementation = function (cr, key) {
            if (key === "android_id") { return PROFILE.android_id; }
            return this.getString(cr, key);
        };
    });

    // ---------- 清 smid 缓存: 读本地持久化 smid 恒返回 "" ----------
    onClassReady("com.ishumei.smantifraud.l11l11Il1l1l", function (C) {
        try {
            C.l1111l111111Il.overload().implementation = function () { return ""; };  // 本地 smid → 空
        } catch (e) { console.log("[fake][!] clear-cache hook: " + e); }
    });

    // ---------- token 注入: 让 app 的 getToken/getUUID 返回 profile.token(与 d 成一套) ----------
    onClassReady("com.mcd.library.utils.AppInfoUtil", function (A) {
        if (PROFILE.token) {
            try { A.getToken.overload("android.content.Context").implementation = function (c) { return PROFILE.token; }; } catch (e) {}
            try { A.getUUID.overload("android.content.Context").implementation = function (c) { return PROFILE.token; }; } catch (e) {}
        }
        // dump 请求头实际用的 mcdToken
        try { A.getMcdDeviceToken.overload().implementation = function () { var r = this.getMcdDeviceToken(); console.log("[hdr] getMcdDeviceToken=" + r); return r; }; } catch (e) {}
    });
    // ---------- dump 真实请求头 token/d: SmSDK.getSmDeviceId 返回=请求头 d, getToken 返回=请求头 token ----------
    onClassReady("com.mcd.library.utils.SmSDK", function (S2) {
        try {
            S2.getSmDeviceId.overload("java.lang.String").implementation = function (p) {
                var r = this.getSmDeviceId(p);
                var tk = null;
                try { tk = Java.use("com.mcd.library.utils.AppInfoUtil").getToken(Java.use("android.app.ActivityThread").currentApplication()); } catch (e) {}
                console.log("[hdr] 请求头 token=" + tk + "  d=" + (r ? r.substring(0, 24) + "…" : r));
                return r;
            };
        } catch (e) {}
    });

    console.log("[fake] 假设备注入完成: android_id=" + PROFILE.android_id + " token=" + (PROFILE.token ? PROFILE.token.substring(0, 8) + "…" : "?") + " model=" + PROFILE.props["ro.product.model"]);
});

// ---------- RPC ----------
function J(fn) { var out = null; Java.perform(function () { out = fn(); }); return out; }
rpc.exports = {
    getd: function () {
        return J(function () {
            try {
                // 触发前把内存 smid 字段清空,确保走 vdata 注册
                Java.use("com.ishumei.smantifraud.l11l11l11I1l").l111l11111Il.value = null;
            } catch (e) {}
            try { return Java.use("com.ishumei.smantifraud.SmAntiFraud").getDeviceId(); } catch (e) { return "ERR:" + e; }
        });
    },
    getdraw: function () {   // 不清内存,拿注册回填后的真实缓存值
        return J(function () {
            try { return Java.use("com.ishumei.smantifraud.SmAntiFraud").getDeviceId(); } catch (e) { return "ERR:" + e; }
        });
    },
    getvdata: function () {
        return J(function () {
            try { return Java.use("com.ishumei.smantifraud.SmAntiFraud").getVData(); } catch (e) { return "ERR:" + e; }
        });
    }
};
