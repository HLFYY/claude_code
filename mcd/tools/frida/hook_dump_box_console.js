/*
 * hook_dump_box_console.js —— dump 数美 SMSDK.v1 入参明文(box/shortbox JSON)
 *   → 直接打到 frida console(清晰分隔,方便复制回传)+ 备份落盘 /sdcard/mcd_box_dump/
 *
 * 和 hook_dump_box_plain.js 的区别:那份用 send() 给 dumpbox_cloud.py 解析;
 *   这份是给【手动 frida -U -l 跑、肉眼复制 console】用的。逻辑/竞速完全一致。
 *
 * ★必须配 frida_bypass_combined_local.js 一起 spawn(bypass 在前):
 *   app 原生壳 libmsaoaidsec 的 DexHelper 要在 25ms 轮询里赶在崩溃陷阱前 patch 完,
 *   只能 2 个脚本(bypass + 本脚本),多一个会抢 JS 线程→必崩。
 *
 * ================= 用法(和你之前给大佬的一样,只换第二个脚本) =================
 *   frida -U -f com.mcdonalds.gma.cn \
 *     -l frida_bypass_combined_local.js \
 *     -l hook_dump_box_console.js
 *
 *   (真机 USB 用 -U;远程/云手机用 -D <设备序列号>)
 *
 *   ★必须让 frida 用 -f 启动(spawn)app,别自己先点开!
 *     数美 SMSDK.v1(造设备/注册)只在【app 冷启动】触发一次——attach 到已开着的 app 抓不到;
 *     且 frida-server 开着时手动开 app 会被原生壳扫到直接崩。
 *     若 app 正开着,先 `adb shell am force-stop com.mcdonalds.gma.cn` 再跑上面命令(-f 也会 force-stop)。
 *     前提:app 之前已点过隐私同意(数美 SDK 才会初始化注册),别 pm clear。
 *
 *   spawn 后脚本会每 0.8s 主动逼一次注册,1~3 秒内就会打出 box。
 *   看到 ===SM-BOX-BEGIN i=2 ...=== 到 ===SM-BOX-END=== 之间那段 JSON 就是模板,整段复制回传。
 *   i=2 = shortbox(注册实际用,核心) / i=1 = 完整 box / i=3 = 服务端配置(忽略)。
 *   备份也会写到 /sdcard/mcd_box_dump/ ,可 adb pull。
 * =========================================================================
 */

// 备份目录:优先 app 自己的 files 目录(app 进程必可写,不受 scoped storage/权限影响),
// 回退 /sdcard。备份只是兜底——console 输出才是主路径(直接复制回传即可)。
var OUT_DIR = null;
function outDir() {
    if (OUT_DIR) return OUT_DIR;
    try {
        var AT = Java.use('android.app.ActivityThread');
        var ctx = AT.currentApplication().getApplicationContext();
        OUT_DIR = ctx.getFilesDir().getAbsolutePath() + '/mcd_box_dump';   // /data/user/0/<pkg>/files/mcd_box_dump
    } catch (e) {
        OUT_DIR = '/sdcard/mcd_box_dump';
    }
    return OUT_DIR;
}
var seen = {};

function saveFile(name, text) {
    try {
        var JFile = Java.use('java.io.File');
        var FOS = Java.use('java.io.FileOutputStream');
        var dir = JFile.$new(outDir());
        if (!dir.exists()) dir.mkdirs();
        var path = outDir() + '/' + name;
        var fos = FOS.$new(path);
        fos.write(Java.use('java.lang.String').$new(text).getBytes('UTF-8'));
        fos.flush(); fos.close();
        console.log('[dump] 备份已存 ' + path + '  (adb 可 su -c cat 取;但直接复制上面 console 更方便)');
    } catch (e) { console.log('[dump] 备份落盘跳过(不影响,复制 console 即可): ' + e); }
}

function emit(i, sv) {
    // 按内容判断类型:字段数 >=50 当完整 box,否则 shortbox
    var n = (sv.match(/"a\d+"\s*:/g) || []).length;
    var kind = (i === 2 || (n > 0 && n < 50)) ? 'SHORTBOX' : (n >= 50 ? 'FULLBOX' : 'BOX');
    // 去重(同一 box 每 0.8s 会被触发多次,只打第一次)
    var key = i + ':' + sv.length;
    if (seen[key]) return;
    seen[key] = true;
    console.log('\n===SM-BOX-BEGIN i=' + i + ' kind=' + kind + ' fields=' + n + ' len=' + sv.length + '===');
    console.log(sv);
    console.log('===SM-BOX-END i=' + i + '===\n');
    saveFile('cloud_v1_arg' + i + '.json', sv);
}

Java.perform(function () {
    // ---- hook SMSDK.v1(SMSDK 类在 spawn 时还没加载,带重试轮询) ----
    var done = false;
    function hookV1(retries) {
        if (done) return;
        try {
            var SMSDK = Java.use('com.ishumei.smantifraud.dfp.SMSDK');
            if (SMSDK.v1) {
                SMSDK.v1.overloads.forEach(function (ov, idx) {
                    ov.implementation = function () {
                        for (var i = 0; i < arguments.length; i++) {
                            var a = arguments[i], sv = null;
                            try { if (a !== null) sv = '' + a; } catch (e) {}
                            // box 入参特征:以 { 开头的长 JSON
                            if (sv && sv.length > 80 && sv.charAt(0) === '{') emit(i, sv);
                        }
                        return this.v1.apply(this, arguments);
                    };
                });
                done = true;
                console.log('[dump] hooked SMSDK.v1 ✓  进登录/发码页或等自动触发');
            }
        } catch (e) {
            if (retries > 0) setTimeout(function () { Java.perform(function () { hookV1(retries - 1); }); }, 300);
        }
    }
    hookV1(80);

    // ---- 主动逼注册(尽早+密集,赶在原生壳崩溃前吐出 v1;每 0.8s,共 30 次) ----
    function trig(n) {
        try { var Z = Java.use('com.ishumei.smantifraud.SmAntiFraud'); Z.getVData(); Z.getDeviceId(); } catch (e) {}
        if (n > 0) setTimeout(function () { Java.perform(function () { trig(n - 1); }); }, 800);
    }
    setTimeout(function () { Java.perform(function () { trig(30); }); }, 1200);
    console.log('[dump] hook_dump_box_console 安装完成,dump 会打印在下方(直接复制 ===SM-BOX-BEGIN...END=== 之间的 JSON 回传)');
});
