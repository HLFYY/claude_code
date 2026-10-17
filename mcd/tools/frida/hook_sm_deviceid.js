/*
 * hook_sm_deviceid.js —— dump 数美设备指纹 d
 * 目标包: com.mcdonalds.gma.cn
 *
 * 运行:
 *   # 附加(推荐，先手动打开APP等它加载完)
 *   frida -U com.mcdonalds.gma.cn -l hook_sm_deviceid.js
 *   # 或 spawn 启动
 *   frida -U -f com.mcdonalds.gma.cn -l hook_sm_deviceid.js
 *
 * 作用: hook
 *   - com.ishumei.smantifraud.SmAntiFraud.getDeviceId()      数美最终 d
 *   - com.mcd.library.utils.SmSDK.getSmDeviceId(path)        mcd 请求头直接取它
 *   d = "B"+smid(注册成功后) 或 "D"+base64(vdata)(注册前/异常)
 *
 * 脱壳/加固包的 dex 运行时才解密，类可能晚加载，故带重试。
 */

function tagOf(v) {
    if (v === null || v === undefined) return '(null)';
    var prefix = v.length > 0 ? v.charAt(0) : '?';
    return '[len=' + v.length + ' prefix=' + prefix + ' => ' + (prefix === 'B' ? 'B+smid(已注册)' : prefix === 'D' ? 'D+base64(vdata,未注册)' : '未知') + ']';
}

// 类就绪后再安装 hook（应对脱壳包 dex 晚加载）
function onClassReady(className, installFn, retries) {
    retries = (retries === undefined) ? 20 : retries;
    try {
        var C = Java.use(className);
        installFn(C);
        console.log('[+] hooked ' + className);
    } catch (e) {
        if (retries > 0) {
            setTimeout(function () { Java.perform(function () { onClassReady(className, installFn, retries - 1); }); }, 500);
        } else {
            console.log('[-] 放弃 ' + className + ' : ' + e);
        }
    }
}

Java.perform(function () {
    var last = { af: undefined, ss: undefined };

    // 1) SmAntiFraud.getDeviceId()  —— 数美返回的最终 d
    onClassReady('com.ishumei.smantifraud.SmAntiFraud', function (SmAntiFraud) {
        SmAntiFraud.getDeviceId.overload().implementation = function () {
            var r = this.getDeviceId();
            if (r !== last.af) {
                last.af = r;
                console.log('\n[d-dump] SmAntiFraud.getDeviceId() =\n' + r + '\n  ' + tagOf(r));
            }
            return r;
        };
        // 注册完成时的异步回调变体（onResult 拿到 "B"+smid）
        try {
            SmAntiFraud.getDeviceId.overload('com.ishumei.smantifraud.SmAntiFraud$IDeviceIdCallback').implementation = function (cb) {
                console.log('[d-dump] SmAntiFraud.getDeviceId(callback) 被调用（异步取 smid）');
                return this.getDeviceId(cb);
            };
        } catch (e) {}
    });

    // 2) SmSDK.getSmDeviceId(path)  —— mcd BFFHeaderInterceptor 直接用它当 d 头
    onClassReady('com.mcd.library.utils.SmSDK', function (SmSDK) {
        SmSDK.getSmDeviceId.overload('java.lang.String').implementation = function (path) {
            var r = this.getSmDeviceId(path);
            if (r !== last.ss) {
                last.ss = r;
                console.log('\n[d-dump] SmSDK.getSmDeviceId("' + path + '") =\n' + r + '\n  ' + tagOf(r));
            }
            return r;
        };
    });

    console.log('[*] d-dump hooks 安装中... 现在在 APP 里走 登录/发送验证码，观察 [d-dump] 输出（值变化才打印，去重）。');
});
