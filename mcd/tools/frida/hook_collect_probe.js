/*
 * hook_collect_probe.js —— 探测数美(ishumei/SmAntiFraud)实际读取了哪些稳定设备标识
 * 思路: hook 各设备标识 Java API,只在调用栈含 ishumei/smantifraud 时打印(去数美噪音)
 *       外加 native __system_property_get 白名单,补 native 直接读的属性
 * 用法: ./getd.sh hook_collect_probe.js 50
 * 结束后看 [collect][SM](数美读的 Java API) 与 [prop](被读的系统属性)
 */

function stack() {
    try {
        var Log = Java.use('android.util.Log');
        var Thr = Java.use('java.lang.Throwable');
        return Log.getStackTraceString(Thr.$new());
    } catch (e) { return ''; }
}
function isSm(s) { return s && (s.indexOf('ishumei') >= 0 || s.indexOf('smantifraud') >= 0); }

var seen = {};
function once(key, line) {
    if (seen[key]) return; seen[key] = 1;
    console.log(line);
}

function hookAll(cls, method, label) {
    try {
        var C = Java.use(cls);
        if (!C[method]) return;
        C[method].overloads.forEach(function (ov) {
            try {
                ov.implementation = function () {
                    var r = ov.apply(this, arguments);
                    var st = stack();
                    if (isSm(st)) {
                        var a = '';
                        if (arguments.length && arguments[0] != null) {
                            try { a = ' arg0=' + arguments[0]; } catch (e) {}
                            if (arguments.length > 1) { try { a += ' arg1=' + arguments[1]; } catch (e) {} }
                        }
                        once(label + a, '[collect][SM] ' + label + a + '  ->  ' + r);
                    }
                    return r;
                };
            } catch (e) {}
        });
    } catch (e) {}
}

Java.perform(function () {
    // ---- 稳定硬件/系统标识 ----
    hookAll('android.provider.Settings$Secure', 'getString', 'Settings.Secure.getString(android_id?)');
    hookAll('android.provider.Settings$System', 'getString', 'Settings.System.getString');
    hookAll('android.provider.Settings$Global', 'getString', 'Settings.Global.getString');
    hookAll('android.telephony.TelephonyManager', 'getDeviceId', 'TM.getDeviceId(IMEI)');
    hookAll('android.telephony.TelephonyManager', 'getImei', 'TM.getImei');
    hookAll('android.telephony.TelephonyManager', 'getMeid', 'TM.getMeid');
    hookAll('android.telephony.TelephonyManager', 'getSubscriberId', 'TM.getSubscriberId(IMSI)');
    hookAll('android.telephony.TelephonyManager', 'getSimSerialNumber', 'TM.getSimSerial');
    hookAll('android.telephony.TelephonyManager', 'getLine1Number', 'TM.getLine1Number');
    hookAll('android.telephony.TelephonyManager', 'getNetworkOperator', 'TM.getNetworkOperator');
    hookAll('android.telephony.TelephonyManager', 'getNetworkOperatorName', 'TM.getOperatorName');
    hookAll('android.net.wifi.WifiInfo', 'getMacAddress', 'WifiInfo.getMacAddress');
    hookAll('android.net.wifi.WifiInfo', 'getBSSID', 'WifiInfo.getBSSID');
    hookAll('android.net.wifi.WifiInfo', 'getSSID', 'WifiInfo.getSSID');
    hookAll('java.net.NetworkInterface', 'getHardwareAddress', 'NetworkInterface.getHardwareAddress(MAC)');
    hookAll('android.os.Build', 'getSerial', 'Build.getSerial');
    hookAll('android.bluetooth.BluetoothAdapter', 'getAddress', 'Bluetooth.getAddress');
    // ---- OAID(各厂商/msa) ----
    hookAll('com.bun.miitmdid.core.MdidSdkHelper', 'InitSdk', 'OAID.MdidSdkHelper.InitSdk');
    // ---- 系统属性(Java 封装) ----
    hookAll('android.os.SystemProperties', 'get', 'SystemProperties.get');
    hookAll('java.lang.System', 'getProperty', 'System.getProperty');
    // ---- 应用列表/账户(设备画像维度) ----
    hookAll('android.app.ApplicationPackageManager', 'getInstalledPackages', 'PM.getInstalledPackages');
    hookAll('android.app.ApplicationPackageManager', 'getInstalledApplications', 'PM.getInstalledApplications');

    // ---- native 属性读白名单(补 native 直接读的) ----
    try {
        var spg = Module.findExportByName(null, '__system_property_get');
        if (spg) {
            var WL = ['ro.serialno', 'ro.boot.serialno', 'ro.product.model', 'ro.product.brand',
                'ro.product.manufacturer', 'ro.product.device', 'ro.build.fingerprint',
                'ro.boot.hardware', 'ro.hardware', 'gsm.sim.operator.numeric',
                'persist.sys.oaid', 'ro.boot.bootloader', 'ro.serialno'];
            Interceptor.attach(spg, {
                onEnter: function (a) { this.k = a[0].readCString(); },
                onLeave: function (r) { if (WL.indexOf(this.k) >= 0) once('prop:' + this.k, '[prop] ' + this.k); }
            });
        }
    } catch (e) {}

    console.log('[*] 采集探针安装完成,8s 后主动触发数美采集(getVData/getDeviceId)…');
    function trigger(n) {
        try {
            var S = Java.use('com.ishumei.smantifraud.SmAntiFraud');
            S.getVData();        // 强制现场采集一次
            S.getDeviceId();
        } catch (e) {}
        if (n > 0) setTimeout(function () { Java.perform(function () { trigger(n - 1); }); }, 3000);
    }
    setTimeout(function () { Java.perform(function () { trigger(5); }); }, 8000);
});
