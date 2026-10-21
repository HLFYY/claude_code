/*
 * hook_fake_device_v2.js —— 路线A 增强版:堵住 v1 的硬件真值泄漏
 * v1 只伪造 Java android_id + native __system_property_get。
 * exp9 探针实测:数美还通过以下路径读到真机真值,把假设备全绑回一台 Pixel:
 *   - __system_property_find / __system_property_read_callback(绕过 _get)读 model/brand/device/name/hardware
 *   - open+read 真实文件:/proc/sys/kernel/random/boot_id(整机唯一,最致命)、/proc/cpuinfo、/proc/net/arp(MAC)
 * v2 新增:
 *   - hook __system_property_read_callback:对 PROFILE.props 里的名字替换成假值(堵 find 路径)
 *   - hook read():对 boot_id/cpuinfo/arp 等目标 fd 返回 INJECT_PROFILE.fake_files 里的假内容(每设备不同)
 * INJECT_PROFILE 需含: android_id, token, props, fake_files{路径:假内容}
 */
var PROFILE = (typeof INJECT_PROFILE !== 'undefined') ? INJECT_PROFILE : {
    android_id: "facefacefaceface",
    token: "facefacefacefacefacefaceface0000",
    props: {
        "ro.serialno": "FAKE1A2B3C4D", "ro.boot.serialno": "FAKE1A2B3C4D",
        "ro.product.model": "M2012K11AC", "ro.product.brand": "Redmi", "ro.product.manufacturer": "Xiaomi",
        "ro.product.device": "alioth", "ro.product.name": "alioth",
        "ro.build.fingerprint": "Redmi/alioth/alioth:11/RKQ1.200826.002/V12.5.1.0.RKHCNXM:user/release-keys",
        "ro.hardware": "qcom", "ro.boot.hardware": "qcom", "ro.boot.bootloader": "unknown"
    },
    fake_files: {}
};
var FAKE_FILES = PROFILE.fake_files || {};

// 覆盖各分区变体
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

var _keep = [];  // 防 NativeCallback / 分配的内存被 GC
var _seenProp = {}, _seenRead = {};  // 命中去重日志

function onClassReady(name, fn, n) {
    n = (n === undefined) ? 40 : n;
    try { fn(Java.use(name)); console.log("[fake] hooked " + name); }
    catch (e) { if (n > 0) setTimeout(function () { Java.perform(function () { onClassReady(name, fn, n - 1); }); }, 400); }
}

// ---------- native: __system_property_get ----------
(function hookPropGet() {
    var spg = Module.findExportByName("libc.so", "__system_property_get");
    if (!spg) { console.log("[fake][!] 找不到 __system_property_get"); return; }
    Interceptor.attach(spg, {
        onEnter: function (a) { this.name = a[0].readCString(); this.vbuf = a[1]; },
        onLeave: function (ret) {
            var v = PROFILE.props[this.name];
            if (v !== undefined && this.vbuf && !this.vbuf.isNull()) {
                if (v.length > 91) v = v.substring(0, 91);
                this.vbuf.writeUtf8String(v);
                ret.replace(ptr(v.length));
            }
        }
    });
    console.log("[fake] native __system_property_get hooked");
})();

// ---------- native: __system_property_read_callback(堵 find 路径) ----------
(function hookPropReadCb() {
    var rcb = Module.findExportByName("libc.so", "__system_property_read_callback");
    if (!rcb) { console.log("[fake][!] 无 __system_property_read_callback(老系统?)"); return; }
    Interceptor.attach(rcb, {
        onEnter: function (args) {
            var origCbPtr = args[1];
            if (origCbPtr.isNull()) return;
            var origCall = new NativeFunction(origCbPtr, 'void', ['pointer', 'pointer', 'pointer', 'uint']);
            var wrapper = new NativeCallback(function (cookie, namePtr, valuePtr, serial) {
                var fake;
                try {
                    var name = namePtr.readCString();
                    fake = PROFILE.props[name];
                } catch (e) { fake = undefined; }
                if (fake !== undefined) {
                    if (fake.length > 91) fake = fake.substring(0, 91);
                    var mem = Memory.allocUtf8String(fake);
                    _keep.push(mem);
                    try { var nm = namePtr.readCString(); if (!_seenProp[nm]) { _seenProp[nm] = 1; console.log("[fake-prop-cb] 命中 " + nm + " -> " + fake); } } catch (e) {}
                    origCall(cookie, namePtr, mem, serial);
                } else {
                    origCall(cookie, namePtr, valuePtr, serial);
                }
            }, 'void', ['pointer', 'pointer', 'pointer', 'uint']);
            _keep.push(wrapper);
            this.context.x1 = wrapper;   // arm64: 第2参数在 x1
        }
    });
    console.log("[fake] native __system_property_read_callback hooked");
})();

// ---------- native: open/openat 路径重定向(把真实敏感文件指向预置假文件) ----------
// FAKE_FILES: { "真实路径": "设备上假文件路径" }。Python 端已把假内容写到假文件(chmod 644)。
// 只改 open 入参指针,不碰 read(),避免全局 read hook 的高频开销/崩溃。
(function hookFiles() {
    if (!Object.keys(FAKE_FILES).length) { console.log("[fake] 无 fake_files,跳过文件伪造"); return; }
    ["open", "openat"].forEach(function (fn) {
        var p = Module.findExportByName("libc.so", fn);
        if (!p) return;
        var idx = (fn === "openat") ? 1 : 0;
        var reg = (idx === 1) ? 'x1' : 'x0';
        Interceptor.attach(p, {
            onEnter: function (a) {
                try {
                    var path = a[idx].readCString();
                    var fk = path && FAKE_FILES[path];
                    if (fk) {
                        var np = Memory.allocUtf8String(fk);
                        _keep.push(np);
                        this.context[reg] = np;
                        if (!_seenRead[path]) { _seenRead[path] = 1; console.log("[fake-file] 重定向 " + path + " -> " + fk); }
                    }
                } catch (e) {}
            }
        });
    });
    console.log("[fake] 文件重定向 hooked: " + Object.keys(FAKE_FILES).join(", "));
})();

Java.perform(function () {
    // ---------- Java: android.os.Build 静态字段(进程启动即定死,native prop hook 改不到;
    //            数美 com.ishumei.smantifraud.l11l1111Ill 直接读 Build.MODEL/BRAND/... 泄漏真机) ----------
    (function fakeBuild() {
        try {
            var B = Java.use("android.os.Build");
            var P = PROFILE.props;
            function setf(field, val) { try { if (val !== undefined && val !== null) B[field].value = val; } catch (e) {} }
            setf("MODEL", P["ro.product.model"]);
            setf("BRAND", P["ro.product.brand"]);
            setf("MANUFACTURER", P["ro.product.manufacturer"]);
            setf("DEVICE", P["ro.product.device"]);
            setf("PRODUCT", P["ro.product.name"]);
            setf("BOARD", P["ro.product.device"]);
            setf("FINGERPRINT", P["ro.build.fingerprint"]);
            setf("HARDWARE", P["ro.hardware"]);
            setf("BOOTLOADER", P["ro.boot.bootloader"]);
            setf("SERIAL", P["ro.serialno"]);
            setf("HOST", "fake-build-host");
            setf("DISPLAY", (P["ro.build.fingerprint"] || "").split("/").length > 3 ? P["ro.build.fingerprint"].split("/")[3] : undefined);
            try { B.getRadioVersion.implementation = function () { return ""; }; } catch (e) {}
            try { B.getSerial.implementation = function () { return P["ro.serialno"] || "unknown"; }; } catch (e) {}
            console.log("[fake] android.os.Build 字段已覆盖: MODEL=" + B.MODEL.value + " BRAND=" + B.BRAND.value + " FP=" + String(B.FINGERPRINT.value).substring(0, 32) + "…");
        } catch (e) { console.log("[fake][!] Build 覆盖失败: " + e); }
    })();

    // ---------- Java: MediaDrm deviceUniqueId(Widevine 硬件级设备唯一ID,pm clear/重启不变,
    //            数美 com.ishumei.smantifraud.l1111l111111Il 读它→把所有假设备连回同一台真机) ----------
    (function fakeMediaDrm() {
        try {
            var wid = PROFILE.widevine_id;   // 每设备随机 hex(device_builder 注入)
            if (!wid) { console.log("[fake][!] 无 widevine_id,MediaDrm 未伪造"); return; }
            var arr = [];
            for (var i = 0; i < wid.length; i += 2) { var v = parseInt(wid.substr(i, 2), 16); arr.push(v > 127 ? v - 256 : v); }
            var MediaDrm = Java.use("android.media.MediaDrm");
            MediaDrm.getPropertyByteArray.implementation = function (name) {
                if (name === "deviceUniqueId") {
                    if (!_seenProp["wid"]) { _seenProp["wid"] = 1; console.log("[fake] MediaDrm deviceUniqueId 命中 -> 假值(len " + arr.length + ")"); }
                    return Java.array("byte", arr);
                }
                return this.getPropertyByteArray(name);
            };
            console.log("[fake] MediaDrm.getPropertyByteArray(deviceUniqueId) hooked");
        } catch (e) { console.log("[fake][!] MediaDrm hook: " + e); }
    })();

    onClassReady("android.provider.Settings$Secure", function (S) {
        S.getString.overload("android.content.ContentResolver", "java.lang.String").implementation = function (cr, key) {
            if (key === "android_id") { return PROFILE.android_id; }
            return this.getString(cr, key);
        };
    });
    onClassReady("com.ishumei.smantifraud.l11l11Il1l1l", function (C) {
        try { C.l1111l111111Il.overload().implementation = function () { return ""; }; }
        catch (e) { console.log("[fake][!] clear-cache hook: " + e); }
    });
    onClassReady("com.mcd.library.utils.AppInfoUtil", function (A) {
        if (PROFILE.token) {
            try { A.getToken.overload("android.content.Context").implementation = function (c) { return PROFILE.token; }; } catch (e) {}
            try { A.getUUID.overload("android.content.Context").implementation = function (c) { return PROFILE.token; }; } catch (e) {}
        }
        try { A.getMcdDeviceToken.overload().implementation = function () { var r = this.getMcdDeviceToken(); console.log("[hdr] getMcdDeviceToken=" + r); return r; }; } catch (e) {}
    });
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
    // ---------- v3 整机环境伪造:堵 SMSDK.v1 明文里泄漏的真机指纹(屏幕/内存/输入法/安装时间/桌面) ----------
    (function fakeEnv() {
        var E = PROFILE.env;
        if (!E) return;
        // 屏幕 a36/a92/a34/a90
        try {
            var Display = Java.use("android.view.Display");
            ["getMetrics", "getRealMetrics"].forEach(function (fn) {
                try {
                    Display[fn].implementation = function (dm) {
                        this[fn](dm);
                        try {
                            dm.widthPixels.value = E.screen.w; dm.heightPixels.value = E.screen.h;
                            dm.densityDpi.value = E.screen.dpi; dm.density.value = E.screen.dpi / 160.0;
                            dm.xdpi.value = E.screen.dpi; dm.ydpi.value = E.screen.dpi;
                        } catch (e) {}
                    };
                } catch (e) {}
            });
            ["getSize", "getRealSize"].forEach(function (fn) {
                try {
                    Display[fn].overload("android.graphics.Point").implementation = function (p) {
                        this[fn](p); try { p.x.value = E.screen.w; p.y.value = E.screen.h; } catch (e) {}
                    };
                } catch (e) {}
            });
        } catch (e) { console.log("[fake][!] screen(Display): " + e); }
        // 屏幕走 Resources 路径 a36/a34(数美实际从这取,Display hook 拦不到)
        try {
            var Resources = Java.use("android.content.res.Resources");
            Resources.getDisplayMetrics.implementation = function () {
                var dm = this.getDisplayMetrics();
                try {
                    dm.widthPixels.value = E.screen.w; dm.heightPixels.value = E.screen.h;
                    dm.densityDpi.value = E.screen.dpi; dm.density.value = E.screen.dpi / 160.0;
                    dm.xdpi.value = E.screen.dpi; dm.ydpi.value = E.screen.dpi;
                } catch (e) {}
                return dm;
            };
        } catch (e) { console.log("[fake][!] screen(Resources): " + e); }
        // 存储 a71/a70/a69(StatFs + File)
        try {
            var TOT = E.total_storage, FREE = Math.floor(E.total_storage * 0.42);
            try {
                var StatFs = Java.use("android.os.StatFs");
                try { StatFs.getTotalBytes.implementation = function () { return TOT; }; } catch (e) {}
                try { StatFs.getAvailableBytes.implementation = function () { return FREE; }; } catch (e) {}
                try { StatFs.getFreeBytes.implementation = function () { return FREE; }; } catch (e) {}
            } catch (e) {}
            var JFile = Java.use("java.io.File");
            JFile.getTotalSpace.implementation = function () { var r = this.getTotalSpace(); return (r > 1000000000) ? TOT : r; };
            JFile.getFreeSpace.implementation = function () { var r = this.getFreeSpace(); return (r > 1000000000) ? FREE : r; };
            JFile.getUsableSpace.implementation = function () { var r = this.getUsableSpace(); return (r > 1000000000) ? FREE : r; };
        } catch (e) { console.log("[fake][!] storage: " + e); }
        // 输入法列表 a63(清空,别暴露真机装的输入法)
        if (E.clear_ime) {
            try {
                var IMM = Java.use("android.view.inputmethod.InputMethodManager");
                var AL = Java.use("java.util.ArrayList");
                try { IMM.getInputMethodList.implementation = function () { return AL.$new(); }; } catch (e) {}
                try { IMM.getEnabledInputMethodList.implementation = function () { return AL.$new(); }; } catch (e) {}
            } catch (e) { console.log("[fake][!] ime: " + e); }
        }
        // 内存 a48
        try {
            var AM = Java.use("android.app.ActivityManager");
            AM.getMemoryInfo.implementation = function (mi) {
                this.getMemoryInfo(mi); try { mi.totalMem.value = E.total_mem; } catch (e) {}
            };
        } catch (e) { console.log("[fake][!] mem: " + e); }
        // 安装/更新时间 a125 + 桌面包名 a154
        try {
            var PM = Java.use("android.app.ApplicationPackageManager");
            PM.getPackageInfo.overload("java.lang.String", "int").implementation = function (pkg, flags) {
                var pi = this.getPackageInfo(pkg, flags);
                try {
                    if (pkg === "com.mcdonalds.gma.cn") {
                        pi.firstInstallTime.value = E.first_install; pi.lastUpdateTime.value = E.last_update;
                    }
                } catch (e) {}
                return pi;
            };
            PM.resolveActivity.overload("android.content.Intent", "int").implementation = function (intent, flags) {
                var ri = this.resolveActivity(intent, flags);
                try {
                    var cats = intent.getCategories();
                    if (ri !== null && cats !== null && cats.contains("android.intent.category.HOME")) {
                        ri.activityInfo.value.packageName.value = E.launcher_pkg;
                        if (!_seenProp["home"]) { _seenProp["home"] = 1; console.log("[fake] 默认桌面 -> " + E.launcher_pkg); }
                    }
                } catch (e) {}
                return ri;
            };
        } catch (e) { console.log("[fake][!] pkginfo/launcher: " + e); }
        console.log("[fake] env 伪造已挂: screen=" + E.screen.w + "x" + E.screen.h + " mem=" + E.total_mem + " launcher=" + E.launcher_pkg);
    })();

    // ---------- 诊断:dump 生成函数 SMSDK.v1 的明文入参(采集到的全部设备字段,加密前) ----------
    if (typeof DUMP_V1 !== "undefined" && DUMP_V1) {
        onClassReady("com.ishumei.smantifraud.dfp.SMSDK", function (S) {
            try {
                S.v1.overload("android.content.Context", "java.lang.String", "java.lang.String", "java.lang.String",
                    "java.lang.String", "java.lang.String", "java.lang.String", "java.lang.String", "long",
                    "java.util.Set", "java.util.List").implementation = function (c, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10) {
                        console.log("[v1-IN-BOX] " + a1);
                        console.log("[v1-IN-SHORTBOX] " + a2);
                        return this.v1(c, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10);
                    };
                console.log("[fake] SMSDK.v1 入参 dump 已挂");
            } catch (e) { console.log("[fake][!] v1 dump hook: " + e); }
        });
    }

    console.log("[fake] v2 假设备注入完成: android_id=" + PROFILE.android_id + " model=" + PROFILE.props["ro.product.model"] + " boot_id=" + ((FAKE_FILES["/proc/sys/kernel/random/boot_id"] || "").trim()));
});

function J(fn) { var out = null; Java.perform(function () { out = fn(); }); return out; }
rpc.exports = {
    getd: function () {
        return J(function () {
            try { Java.use("com.ishumei.smantifraud.l11l11l11I1l").l111l11111Il.value = null; } catch (e) {}
            try { return Java.use("com.ishumei.smantifraud.SmAntiFraud").getDeviceId(); } catch (e) { return "ERR:" + e; }
        });
    },
    getdraw: function () { return J(function () { try { return Java.use("com.ishumei.smantifraud.SmAntiFraud").getDeviceId(); } catch (e) { return "ERR:" + e; } }); },
    getvdata: function () { return J(function () { try { return Java.use("com.ishumei.smantifraud.SmAntiFraud").getVData(); } catch (e) { return "ERR:" + e; } }); }
};
