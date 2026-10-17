/*
 * hook_crypto_probe.js —— 定位数美 vdata 的加密边界,dump 加密前明文
 * 目的: 判断 compress:3 + encode:2 发生在 Java(JCE/zip,可复用) 还是 native(libsmsdk)
 *       并抓到完整明文 vdata JSON(构造伪造设备时照此填字段)
 * 用法: ./getd.sh hook_crypto_probe.js 50
 */
function isSm(s) { return s && (s.indexOf('ishumei') >= 0 || s.indexOf('smantifraud') >= 0); }
function st() {
    try { return Java.use('android.util.Log').getStackTraceString(Java.use('java.lang.Throwable').$new()); }
    catch (e) { return ''; }
}
function toStr(b) { try { return Java.use('java.lang.String').$new(b); } catch (e) { return '(bin)'; } }
function head(b, n) {
    n = n || 64; var s = '';
    for (var i = 0; i < Math.min(b.length, n); i++) { var x = (b[i] & 0xff).toString(16); s += (x.length < 2 ? '0' : '') + x; }
    return s;
}

Java.perform(function () {
    // ---- 标准 JCE 加密 ----
    try {
        var Cipher = Java.use('javax.crypto.Cipher');
        Cipher.getInstance.overload('java.lang.String').implementation = function (t) {
            if (isSm(st())) console.log('[crypto] Cipher.getInstance("' + t + '")');
            return this.getInstance(t);
        };
        Cipher.doFinal.overload('[B').implementation = function (b) {
            var sm = isSm(st()); var r = this.doFinal(b);
            if (sm) {
                console.log('\n[crypto] Cipher.doFinal  in=' + b.length + 'B  out=' + r.length + 'B');
                var p = toStr(b);
                console.log('   明文(str): ' + (p.length > 800 ? p.substring(0, 800) + '…' : p));
                console.log('   明文(hex头): ' + head(b));
            }
            return r;
        };
    } catch (e) { console.log('[!] Cipher hook 失败 ' + e); }

    // ---- 压缩(compress:3 可能是 deflate/gzip) ----
    try {
        var Deflater = Java.use('java.util.zip.Deflater');
        Deflater.setInput.overload('[B').implementation = function (b) {
            if (isSm(st())) {
                console.log('\n[zip] Deflater.setInput ' + b.length + 'B');
                var p = toStr(b);
                console.log('   压缩前明文: ' + (p.length > 800 ? p.substring(0, 800) + '…' : p));
            }
            return this.setInput(b);
        };
    } catch (e) {}
    try {
        var GZIP = Java.use('java.util.zip.GZIPOutputStream');
        GZIP.write.overload('[B', 'int', 'int').implementation = function (b, o, l) {
            if (isSm(st())) console.log('\n[zip] GZIPOutputStream.write ' + l + 'B  明文: ' + toStr(b).substring(0, 400));
            return this.write(b, o, l);
        };
    } catch (e) {}

    // ---- Base64(数美最后一步) ----
    try {
        var B64 = Java.use('android.util.Base64');
        B64.encodeToString.overload('[B', 'int').implementation = function (b, f) {
            var r = this.encodeToString(b, f);
            if (isSm(st()) && b.length > 32) {
                console.log('\n[b64] encodeToString in=' + b.length + 'B  是否JSON头: ' + toStr(b).substring(0, 1).replace(/[^\x20-\x7e]/, '?'));
            }
            return r;
        };
    } catch (e) {}

    console.log('[*] 加密探针安装完成,8s 后主动触发 getVData/getDeviceId …');
    function trig(n) {
        try { var S = Java.use('com.ishumei.smantifraud.SmAntiFraud'); S.getVData(); S.getDeviceId(); } catch (e) {}
        if (n > 0) setTimeout(function () { Java.perform(function () { trig(n - 1); }); }, 3000);
    }
    setTimeout(function () { Java.perform(function () { trig(4); }); }, 8000);
});
