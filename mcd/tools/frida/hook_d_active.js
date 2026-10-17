/*
 * hook_d_active.js —— 主动触发 + RPC 暴露 数美 d 值
 * 目标: com.mcdonalds.gma.cn
 *
 * 1) 启动后延迟主动调用 SmAntiFraud.getDeviceId() / getVData() / SmSDK.getSmDeviceId()
 *    直接把 d 打印出来（不依赖 app 自己发请求）
 * 2) rpc.exports 暴露 getd / getvdata / getsmdid，供 python 远程调用（第二步 RPC 方案基础）
 * 3) 同时 hook SmOption 所有 setter，dump 初始化参数
 */

function J(fn) { Java.perform(fn); }

// ---------- RPC 导出（python 侧 script.exports.getd() 调用）----------
rpc.exports = {
    getd: function () {
        var out = null;
        J(function () {
            try { out = Java.use('com.ishumei.smantifraud.SmAntiFraud').getDeviceId(); } catch (e) { out = 'ERR:' + e; }
        });
        return out;
    },
    getvdata: function () {
        var out = null;
        J(function () {
            try { out = Java.use('com.ishumei.smantifraud.SmAntiFraud').getVData(); } catch (e) { out = 'ERR:' + e; }
        });
        return out;
    },
    getsmdid: function (path) {
        var out = null;
        J(function () {
            try { out = Java.use('com.mcd.library.utils.SmSDK').INSTANCE.value.getSmDeviceId(path || '/bff/portal/v1/home'); } catch (e) { out = 'ERR:' + e; }
        });
        return out;
    }
};

function tag(v) {
    if (v == null) return '(null)';
    var p = v.length ? v.charAt(0) : '?';
    var mean = p === 'B' ? 'B+smid(服务端已注册)' : p === 'D' ? 'D+base64(vdata本地盒子,未拿到smid)' : '未知';
    return '[len=' + v.length + ' prefix=' + p + ' => ' + mean + ']';
}

// ---------- 主动触发（延迟等 create 初始化完成，带重试）----------
// 异常态特征：getVData()=="DU21PcHRpb24gaXMgbnVsbC4="(SmOption is null) 或 d 内含 "not init"
function isNotReady(S) {
    try {
        var v = S.getVData();
        if (v == null || v === 'DU21PcHRpb24gaXMgbnVsbC4=') return true;
    } catch (e) { return true; }
    try {
        var d = S.getDeviceId();
        if (d == null) return true;
        if (d.charAt(0) === 'D') {
            // 解码 data 层判断是否异常态
            var outer = JSON.parse(stringFromB64(d.substring(1)));
            var data = JSON.parse(stringFromB64(outer.data));
            if (data && (data.a1 === 'exception' || data.e)) return true;
        }
    } catch (e) {}
    return false;
}
function stringFromB64(b64) {
    var StringCls = Java.use('java.lang.String');
    var B64 = Java.use('android.util.Base64');
    return StringCls.$new(B64.decode(b64, 0)).toString();
}
function activeDump(retries) {
    J(function () {
        try {
            var S = Java.use('com.ishumei.smantifraud.SmAntiFraud');
            if (isNotReady(S) && retries > 0) {
                setTimeout(function () { activeDump(retries - 1); }, 2000);
                return;
            }
            var d = S.getDeviceId();
            if (d == null && retries > 0) {
                setTimeout(function () { activeDump(retries - 1); }, 2000);
                return;
            }
            var v = null; try { v = S.getVData(); } catch (e) {}
            console.log('\n==================== [d-active] ====================');
            console.log('[d-active] SmAntiFraud.getDeviceId() =\n  ' + d + '\n  ' + tag(d));
            console.log('[d-active] SmAntiFraud.getVData()     =\n  ' + v + '\n  ' + tag(v));
            try {
                var SmSDK = Java.use('com.mcd.library.utils.SmSDK');
                var sd = SmSDK.INSTANCE.value.getSmDeviceId('/bff/portal/v1/home');
                console.log('[d-active] SmSDK.getSmDeviceId(path) =\n  ' + sd + '\n  ' + tag(sd));
            } catch (e) { console.log('[d-active] SmSDK 调用异常: ' + e); }
            console.log('[d-active] SDK 版本: ' + S.getSDKVersion());
            console.log('====================================================\n');
        } catch (e) {
            if (retries > 0) setTimeout(function () { activeDump(retries - 1); }, 1500);
            else console.log('[d-active][ERR] ' + e);
        }
    });
}

// ---------- dump SmOption 初始化参数 ----------
function hookOption() {
    J(function () {
        try {
            var Opt = Java.use('com.ishumei.smantifraud.SmAntiFraud$SmOption');
            ['setOrganization', 'setAppId', 'setPublicKey', 'setArea'].forEach(function (m) {
                try {
                    Opt[m].overload('java.lang.String').implementation = function (x) {
                        console.log('[SmOption] ' + m + '("' + x + '")');
                        return this[m](x);
                    };
                } catch (e) {}
            });
        } catch (e) {}
    });
}

J(function () {
    hookOption();
    console.log('[*] hook_d_active 安装完成，4s 后主动调用 getDeviceId / getVData …');
    setTimeout(function () { activeDump(20); }, 4000);
});
