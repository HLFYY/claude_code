/*
 * hook_dump_box_plain.js —— dump 数美 SMSDK.v1 入参明文(box/shortbox JSON) + 加密边界 + 主动触发
 *
 * 用途: 在"能过数美风控的设备"(如多多云手机)上抓一份干净 box/shortbox 明文模板,
 *       喂 mcd_project/local_device_gen.py 本地批量造设备。
 *
 * 为什么是单脚本(关键): app 原生壳 libmsaoaidsec 的 DexHelper 要被 bypass 在 25ms 轮询里
 *   赶在崩溃陷阱前 patch 完;**多加一个 hook 脚本会抢 JS 线程 → 每次都没 patch 完就崩**。
 *   所以把 v1 入参 dump + crypto 探针 + 主动触发全并进这一个 Java.perform。
 *   配合 frida_bypass_combined_local.js 一起 spawn(bypass 在前)。
 *
 * 用 send() 输出(driver 打印 send); 命中 send 的 __dump=v1-arg 即明文模板。
 *
 * 运行见同目录 dumpbox_cloud.py(spawn+bypass+本脚本+重试竞速)。
 *
 * SMSDK.v1(Context, String×7, long, Set, List): arg1=完整box(~82字段),
 *   arg2=shortbox(~29字段,注册实际用它), arg3=服务端配置(非设备数据)。
 */
function S(tag, obj) { send({ __dump: tag, data: obj }); }
function isSm(s) { return s && (s.indexOf('ishumei') >= 0 || s.indexOf('smantifraud') >= 0); }
function st() {
    try { return Java.use('android.util.Log').getStackTraceString(Java.use('java.lang.Throwable').$new()); }
    catch (e) { return ''; }
}
function toStr(b) { try { return Java.use('java.lang.String').$new(b); } catch (e) { return '(bin)'; } }

Java.perform(function () {
    // ---- SMSDK.v1 入参明文(最重要) ----
    var v1done = false;
    function hookV1(retries) {
        if (v1done) return;
        try {
            var SMSDK = Java.use('com.ishumei.smantifraud.dfp.SMSDK');
            if (SMSDK.v1) {
                SMSDK.v1.overloads.forEach(function (ov, idx) {
                    S('v1-sig', { ov: idx, n: ov.argumentTypes.length });
                    ov.implementation = function () {
                        for (var i = 0; i < arguments.length; i++) {
                            var a = arguments[i];
                            var sv = null;
                            try { if (a !== null) sv = '' + a; } catch (e) {}
                            if (sv && sv.length > 80 && sv.charAt(0) === '{') {
                                S('v1-arg', { ov: idx, i: i, len: sv.length, val: sv });
                            }
                        }
                        return this.v1.apply(this, arguments);
                    };
                });
                v1done = true;
                S('hooked', 'SMSDK.v1');
            }
        } catch (e) {
            if (retries > 0) setTimeout(function () { Java.perform(function () { hookV1(retries - 1); }); }, 300);
        }
    }
    hookV1(80);

    // ---- AES/RSA 明文边界(compress 在 native, Java 层这里拿到的是压缩后二进制; 仅作佐证) ----
    try {
        var Cipher = Java.use('javax.crypto.Cipher');
        Cipher.doFinal.overload('[B').implementation = function (b) {
            var sm = isSm(st()); var r = this.doFinal(b);
            if (sm) {
                var p = toStr(b);
                if (p.length > 0 && p.charAt(0) === '{') S('cipher-plain', { len: b.length, val: p });
            }
            return r;
        };
    } catch (e) {}

    // ---- Java 层 deflate(若 compress 在 Java 则这里出明文; 实测数美走 native, 一般不触发) ----
    try {
        var Deflater = Java.use('java.util.zip.Deflater');
        Deflater.setInput.overload('[B').implementation = function (b) {
            if (isSm(st())) {
                var p = toStr(b);
                S('deflate-plain', { len: b.length, val: (p.length > 4000 ? p.substring(0, 4000) + '…' : p) });
            }
            return this.setInput(b);
        };
    } catch (e) {}

    // ---- 主动触发注册(尽早+密集,赶在崩溃前逼出 v1; 每 0.8s 一次, 共 30 次) ----
    function trig(n) {
        try { var Z = Java.use('com.ishumei.smantifraud.SmAntiFraud'); Z.getVData(); Z.getDeviceId(); } catch (e) {}
        if (n > 0) setTimeout(function () { Java.perform(function () { trig(n - 1); }); }, 800);
    }
    setTimeout(function () { Java.perform(function () { trig(30); }); }, 1200);
    S('ready', 'hook_dump_box_plain 安装完成');
});
