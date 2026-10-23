'use strict';

// 整个脚本包在 IIFE 里：frida 的多个 -l 文件会被拼成同一份脚本，
// 顶层 const 会和 frida_bypass_combined_local.js 的 TAG 撞名。
(function () {

/*
 * 让 AVD（userdebug/test-keys + qemu 特征）在属性层面伪装成一台
 * 正式版（user/release-keys）真机，用于验证 mcd App 的 root/环境检测
 * 到底看的是构建属性，还是更深层的东西。
 *
 * 用法（spawn 模式，必须抢在 数美 SDK 初始化前）：
 *   adb forward tcp:27042 tcp:27042
 *   frida -H 127.0.0.1:27042 -f com.mcdonalds.gma.cn \
 *     -l frida_bypass_combined_local.js -l mcd_env_spoof.js --no-pause
 *
 * 只改"看起来像什么"，不动任何文件，退出即恢复。
 */

const FAKE_PROPS = {
  'ro.build.tags': 'release-keys',
  'ro.build.type': 'user',
  'ro.debuggable': '0',
  'ro.secure': '1',
  'ro.adb.secure': '1',
  'ro.build.fingerprint':
    'google/sunfish/sunfish:11/RQ3A.211001.001/7641976:user/release-keys',
  'ro.bootimage.build.fingerprint':
    'google/sunfish/sunfish:11/RQ3A.211001.001/7641976:user/release-keys',
  'ro.build.description':
    'sunfish-user 11 RQ3A.211001.001 7641976 release-keys',
  'ro.build.flavor': 'sunfish-user',
  'ro.build.product': 'sunfish',
  'ro.product.brand': 'google',
  'ro.product.name': 'sunfish',
  'ro.product.device': 'sunfish',
  'ro.product.model': 'Pixel 4a',
  'ro.product.manufacturer': 'Google',
  'ro.hardware': 'sunfish',
  'ro.board.platform': 'sm7250',
  'ro.kernel.qemu': '',
  'ro.boot.qemu': '',
  'ro.boot.qemu.avd_name': '',
  'ro.hardware.egl': 'adreno',
  'ro.hardware.vulkan': 'adreno',
  'ro.serialno': '1A2B3C4D5E',
  'ro.boot.serialno': '1A2B3C4D5E',
  'ro.boot.verifiedbootstate': 'green',
  'ro.boot.veritymode': 'enforcing',
  'ro.boot.warranty_bit': '0',
  'ro.warranty_bit': '0',
  'init.svc.qemud': 'stopped',
  'init.svc.qemu-props': 'stopped',
  'service.adb.tcp.port': '',
  'gsm.version.baseband': 'MPSS.AT.4.0.c2-00063',
  'ro.csc.sales_code': '',
};

const TAG = '[env-spoof]';
function hit(k, v) {
  console.log(TAG + ' ' + k + ' -> ' + v);
}

/* ---------- 1) libc 层：__system_property_get ---------- */
function hookPropGet() {
  const addr = Module.findExportByName(null, '__system_property_get');
  if (addr === null) {
    console.log(TAG + ' __system_property_get not found');
    return;
  }
  Interceptor.attach(addr, {
    onEnter(args) {
      this.key = null;
      this.buf = args[1];
      try { this.key = args[0].readCString(); } catch (_) {}
    },
    onLeave(ret) {
      if (this.key === null || this.buf.isNull()) return;
      const value = FAKE_PROPS[this.key];
      if (value === undefined) return;
      try {
        this.buf.writeUtf8String(value);
        ret.replace(value.length);
        hit(this.key, value);
      } catch (e) {
        console.log(TAG + ' write failed for ' + this.key + ': ' + e);
      }
    },
  });
  console.log(TAG + ' hooked __system_property_get');
}

/* ---------- 2) libc 层：__system_property_read（旧接口） ---------- */
function hookPropRead() {
  const addr = Module.findExportByName(null, '__system_property_read');
  if (addr === null) return;
  Interceptor.attach(addr, {
    onEnter(args) {
      this.valBuf = args[2];
      this.nameBuf = args[1];
    },
    onLeave(ret) {
      try {
        if (this.nameBuf.isNull()) return;
        const key = this.nameBuf.readCString();
        const value = FAKE_PROPS[key];
        if (value === undefined || this.valBuf.isNull()) return;
        this.valBuf.writeUtf8String(value);
        hit(key, value);
      } catch (_) {}
    },
  });
  console.log(TAG + ' hooked __system_property_read');
}

/* ---------- 3) Java 层：SystemProperties.native_get* + adb 开关 ---------- */
function hookJavaProps() {
  if (!Java.available) return;
  Java.perform(function () {
    try {
      const SP = Java.use('android.os.SystemProperties');

      const mapGet = function (key, fallback) {
        const v = FAKE_PROPS[key];
        if (v === undefined) return fallback;
        hit('java:' + key, v);
        return v;
      };

      SP.native_get.overload('java.lang.String').implementation = function (k) {
        const v = FAKE_PROPS[k];
        return v === undefined ? this.native_get(k) : mapGet(k, v);
      };
      SP.native_get.overload('java.lang.String', 'java.lang.String')
        .implementation = function (k, def) {
          const v = FAKE_PROPS[k];
          return v === undefined ? this.native_get(k, def) : mapGet(k, v);
        };
      SP.native_get_int.overload('java.lang.String', 'int')
        .implementation = function (k, def) {
          const v = FAKE_PROPS[k];
          if (v === undefined) return this.native_get_int(k, def);
          const parsed = parseInt(v, 10);
          return isNaN(parsed) ? def : parsed;
        };
      SP.native_get_boolean.overload('java.lang.String', 'boolean')
        .implementation = function (k, def) {
          const v = FAKE_PROPS[k];
          return v === undefined ? this.native_get_boolean(k, def) : v === '1';
        };
      console.log(TAG + ' hooked android.os.SystemProperties');
    } catch (e) {
      console.log(TAG + ' SystemProperties hook failed: ' + e);
    }

    // ADB 调试开关也算弱信号，一并伪装
    try {
      const G = Java.use('android.provider.Settings$Global');
      const getInt = G.getInt.overload('android.content.ContentResolver',
        'java.lang.String', 'int');
      getInt.implementation = function (cr, name, def) {
        if (name === 'adb_enabled' || name === 'development_settings_enabled') {
          hit('settings:' + name, '0');
          return 0;
        }
        return getInt.call(this, cr, name, def);
      };
      console.log(TAG + ' hooked Settings.Global.getInt');
    } catch (e) {
      console.log(TAG + ' Settings hook failed: ' + e);
    }

    // 关键：Build.* 是静态 final 字段，在 zygote fork 时就已初始化，
    // 靠 hook SystemProperties 根本改不到它们。必须直接改字段值。
    try {
      const B = Java.use('android.os.Build');
      const FIELDS = {
        TAGS: 'release-keys',
        TYPE: 'user',
        FINGERPRINT:
          'google/sunfish/sunfish:11/RQ3A.211001.001/7641976:user/release-keys',
        MODEL: 'Pixel 4a',
        MANUFACTURER: 'Google',
        BRAND: 'google',
        DEVICE: 'sunfish',
        PRODUCT: 'sunfish',
        HARDWARE: 'sunfish',
        BOARD: 'sm7250',
        DISPLAY: 'RQ3A.211001.001',
        ID: 'RQ3A.211001.001',
        USER: 'android-build',
        HOST: 'abfarm-release',
        BOOTLOADER: 'unknown',
        SERIAL: '1A2B3C4D5E',
      };
      let ok = 0;
      Object.keys(FIELDS).forEach(function (name) {
        try {
          B[name].value = FIELDS[name];
          ok++;
        } catch (e) {
          console.log(TAG + ' Build.' + name + ' set failed: ' + e);
        }
      });
      console.log(TAG + ' Build fields set: ' + ok + '/' +
        Object.keys(FIELDS).length);
      try {
        hit('Build.TAGS(readback)', B.TAGS.value);
      } catch (_) {}
    } catch (e) {
      console.log(TAG + ' Build hook failed: ' + e);
    }
  });
}

setImmediate(function () {
  console.log(TAG + ' arch=' + Process.arch + ' pid=' + Process.id);
  hookPropGet();
  hookPropRead();
  hookJavaProps();
});

})();
