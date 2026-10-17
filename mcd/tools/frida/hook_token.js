/* hook_token.js —— 看真机 token 实际值与来源(确认 token 是否设备级稳定、与 d 的关系) */
function onReady(name, fn, n) {
    n = (n === undefined) ? 30 : n;
    try { fn(Java.use(name)); console.log('[+] hooked ' + name); }
    catch (e) { if (n > 0) setTimeout(function () { Java.perform(function () { onReady(name, fn, n - 1); }); }, 400); }
}
Java.perform(function () {
    onReady('com.mcd.library.utils.AppInfoUtil', function (A) {
        A.getToken.overload('android.content.Context').implementation = function (c) {
            var r = this.getToken(c); console.log('[token] getToken() = ' + r); return r;
        };
        A.getMcdDeviceToken.overload().implementation = function () {
            var r = this.getMcdDeviceToken(); console.log('[token] getMcdDeviceToken() = ' + r); return r;
        };
        try {
            A.getDeviceId.overload('android.content.Context').implementation = function (c) {
                var r = this.getDeviceId(c); console.log('[token] getDeviceId(内部,IMEI/设备ID) = ' + r); return r;
            };
        } catch (e) {}
        try {
            A.getUUID.overload('android.content.Context').implementation = function (c) {
                var r = this.getUUID(c); console.log('[token] getUUID(持久化UUID) = ' + r); return r;
            };
        } catch (e) {}
    });
    console.log('[*] token 探针安装,在 app 里走请求/登录触发,观察 [token] 值是否稳定');
});
