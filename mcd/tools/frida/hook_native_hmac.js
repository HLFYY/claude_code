// Hook Native 层 HMAC 函数 - 直接捕获签名消息
// 目标：libsscronet.so 或其他 SO 库中的 HMAC-SHA256 函数

Java.perform(function() {
    console.log("\n" + "=".repeat(100));
    console.log("Native HMAC Hook - 开始");
    console.log("=".repeat(100));

    // ========== 第一部分：提取 SecBox 密钥 ==========
    try {
        var SecBox = Java.use("com.mcd.secbox.SecBox");
        var instance = SecBox.INSTANCE.value;

        console.log("\n[SecBox 密钥]");
        console.log("-".repeat(100));

        var keys = {};
        try { keys.aesKey = instance.getAesKey(); } catch(e) {}
        try { keys.signKey = instance.getSignKey(); } catch(e) {}
        try { keys.v4ak = instance.getV4ak(); } catch(e) {}
        try { keys.v4sk = instance.getV4sk(); } catch(e) {}

        console.log("aesKey  : " + (keys.aesKey || "N/A"));
        console.log("signKey : " + (keys.signKey || "N/A"));
        console.log("v4ak    : " + (keys.v4ak || "N/A"));
        console.log("v4sk    : " + (keys.v4sk || "N/A"));
    } catch(e) {
        console.log("[ERROR] SecBox 提取失败: " + e);
    }

    // ========== 第二部分：Hook Native HMAC 函数 ==========
    console.log("\n[Native HMAC Hook]");
    console.log("-".repeat(100));

    // 加载所有可能包含 HMAC 的库
    var possibleLibs = [
        "libsscronet.so",
        "libcsiipowerenter.so",
        "libdexjni.so",
        "libcrypto.so",
        "libssl.so"
    ];

    possibleLibs.forEach(function(libName) {
        try {
            var lib = Process.getModuleByName(libName);
            console.log("✓ 找到库: " + libName + " @ " + lib.base);

            // Hook HMAC_Init_ex
            try {
                var HMAC_Init_ex = Module.findExportByName(libName, "HMAC_Init_ex");
                if (HMAC_Init_ex) {
                    Interceptor.attach(HMAC_Init_ex, {
                        onEnter: function(args) {
                            this.key = args[2];
                            this.keyLen = args[3].toInt32();
                            console.log("\n[HMAC_Init_ex] 密钥长度: " + this.keyLen);
                            if (this.keyLen > 0 && this.keyLen < 200) {
                                var keyData = Memory.readUtf8String(this.key, this.keyLen);
                                console.log("[HMAC_Init_ex] 密钥: " + keyData);
                            }
                        }
                    });
                    console.log("  ✓ Hook HMAC_Init_ex");
                }
            } catch(e) {}

            // Hook HMAC_Update
            try {
                var HMAC_Update = Module.findExportByName(libName, "HMAC_Update");
                if (HMAC_Update) {
                    Interceptor.attach(HMAC_Update, {
                        onEnter: function(args) {
                            var data = args[1];
                            var len = args[2].toInt32();

                            if (len > 50 && len < 2000) {  // 可能是签名消息
                                console.log("\n" + "*".repeat(100));
                                console.log("[HMAC_Update] 数据长度: " + len + " 字节");

                                try {
                                    var message = Memory.readUtf8String(data, len);
                                    console.log("\n[待签名消息]");
                                    console.log(message);
                                    console.log("\n[消息可视化] (用 | 表示 \\n):");
                                    console.log(message.replace(/\n/g, '\n|\n'));

                                    // 打印十六进制
                                    console.log("\n[十六进制前 200 字节]:");
                                    var hexData = Memory.readByteArray(data, Math.min(len, 200));
                                    console.log(hexdump(hexData, {
                                        offset: 0,
                                        length: Math.min(len, 200),
                                        header: false,
                                        ansi: false
                                    }));
                                } catch(e) {
                                    console.log("[ERROR] 读取消息失败: " + e);
                                }
                                console.log("*".repeat(100) + "\n");
                            }
                        }
                    });
                    console.log("  ✓ Hook HMAC_Update");
                }
            } catch(e) {}

            // Hook HMAC_Final
            try {
                var HMAC_Final = Module.findExportByName(libName, "HMAC_Final");
                if (HMAC_Final) {
                    Interceptor.attach(HMAC_Final, {
                        onLeave: function(retval) {
                            console.log("\n[HMAC_Final] 签名计算完成");
                        }
                    });
                    console.log("  ✓ Hook HMAC_Final");
                }
            } catch(e) {}

        } catch(e) {
            // 库不存在
        }
    });

    // ========== 第三部分：Hook Java 层拦截器（备用） ==========
    try {
        var MCDSignatureInterceptor = Java.use("qf.e");

        MCDSignatureInterceptor.intercept.implementation = function(chain) {
            var request = chain.request();
            var url = request.url();

            console.log("\n" + "=".repeat(100));
            console.log("[Java 拦截器] " + request.method() + " " + url.toString());
            console.log("=".repeat(100));

            var result = this.intercept(chain);

            var newHeaders = result.request().headers();
            console.log("Authorization: " + newHeaders.get("authorization"));
            console.log("X-Hmac-Digest: " + newHeaders.get("x-hmac-digest"));
            console.log("=".repeat(100) + "\n");

            return result;
        };
    } catch(e) {}

    console.log("\n" + "=".repeat(100));
    console.log("Hook 安装完成！等待应用发起请求...");
    console.log("=".repeat(100) + "\n");
});
