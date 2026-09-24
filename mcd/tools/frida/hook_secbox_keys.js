/*
 * Hook SecBox to extract v4ak, v4sk, and aesKey
 *
 * Usage:
 *   frida -U -f com.mcdonalds.gma.cn \
 *     -l frida_bypass_combined_local.js \
 *     -l hook_secbox_keys.js \
 *     --no-pause
 */

console.log('[SecBox Hook] Waiting for SecBox initialization...');

// Hook Java layer
Java.perform(function() {
    try {
        var SecBox = Java.use('com.mcd.secbox.SecBox');

        // Hook getV4ak()
        SecBox.getV4ak.implementation = function() {
            var result = this.getV4ak();
            console.log('[SecBox Hook] ✅ v4ak = ' + result);
            return result;
        };

        // Hook getV4sk()
        SecBox.getV4sk.implementation = function() {
            var result = this.getV4sk();
            console.log('[SecBox Hook] ✅ v4sk = ' + result);
            return result;
        };

        // Hook getAesKey()
        SecBox.getAesKey.implementation = function() {
            var result = this.getAesKey();
            console.log('[SecBox Hook] ✅ aesKey = ' + result);
            return result;
        };

        // Hook init() to catch initialization
        SecBox.init.overload().implementation = function() {
            console.log('[SecBox Hook] SecBox.init() called');
            var result = this.init();

            // Try to get keys immediately after init
            try {
                var v4ak = this.getV4ak();
                var v4sk = this.getV4sk();
                var aesKey = this.getAesKey();

                console.log('[SecBox Hook] ========== KEYS EXTRACTED ==========');
                console.log('[SecBox Hook] v4ak   = ' + v4ak);
                console.log('[SecBox Hook] v4sk   = ' + v4sk);
                console.log('[SecBox Hook] aesKey = ' + aesKey);
                console.log('[SecBox Hook] =====================================');
            } catch (e) {
                console.log('[SecBox Hook] Could not get keys immediately: ' + e);
            }

            return result;
        };

        console.log('[SecBox Hook] ✅ SecBox hooks installed');

    } catch (e) {
        console.log('[SecBox Hook] ❌ Failed to hook SecBox: ' + e);
    }
});

// Also try to hook JNI layer
try {
    var jniFunc = Module.findExportByName(null, 'Java_com_mcd_secbox_JniLib0_cV');
    if (jniFunc) {
        Interceptor.attach(jniFunc, {
            onEnter: function(args) {
                console.log('[SecBox Hook] JniLib0.cV() called - this loads keys from native');
            },
            onLeave: function(retval) {
                console.log('[SecBox Hook] JniLib0.cV() completed');
            }
        });
        console.log('[SecBox Hook] JNI hook installed');
    } else {
        console.log('[SecBox Hook] JNI function not found, will hook at Java layer only');
    }
} catch (e) {
    console.log('[SecBox Hook] Could not hook JNI: ' + e);
}

console.log('[SecBox Hook] Script loaded, waiting for SecBox to initialize...');
