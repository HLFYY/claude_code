// 麦当劳完整签名 Hook - 最全面的数据抓取
// 目标：一次性获取所有签名相关的数据，包括密钥、消息、签名结果

Java.perform(function() {
    console.log("\n" + "=".repeat(100));
    console.log("麦当劳签名完整 Hook - 开始");
    console.log("=".repeat(100));

    // ========== 第一部分：提取所有 SecBox 密钥 ==========
    try {
        var SecBox = Java.use("com.mcd.secbox.SecBox");
        console.log("\n[1] SecBox 密钥提取");
        console.log("-".repeat(100));

        var instance = SecBox.INSTANCE.value;

        var keys = {
            aesKey: "N/A",
            signKey: "N/A",
            v4ak: "N/A",
            v4sk: "N/A",
            wskey: "N/A",
            tmsk: "N/A",
            taAppId: "N/A",
            taSID: "N/A",
            taSK: "N/A"
        };

        try { keys.aesKey = instance.getAesKey(); } catch(e) { keys.aesKey = "[ERROR: " + e + "]"; }
        try { keys.signKey = instance.getSignKey(); } catch(e) { keys.signKey = "[ERROR: " + e + "]"; }
        try { keys.v4ak = instance.getV4ak(); } catch(e) { keys.v4ak = "[ERROR: " + e + "]"; }
        try { keys.v4sk = instance.getV4sk(); } catch(e) { keys.v4sk = "[ERROR: " + e + "]"; }
        try { keys.wskey = instance.getWskey(); } catch(e) { keys.wskey = "[ERROR: " + e + "]"; }
        try { keys.tmsk = instance.getTMSK(); } catch(e) { keys.tmsk = "[ERROR: " + e + "]"; }
        try { keys.taAppId = instance.getTaAppId(); } catch(e) { keys.taAppId = "[ERROR: " + e + "]"; }
        try { keys.taSID = instance.getTaSID(); } catch(e) { keys.taSID = "[ERROR: " + e + "]"; }
        try { keys.taSK = instance.getTaSK(); } catch(e) { keys.taSK = "[ERROR: " + e + "]"; }

        console.log("aesKey    : " + keys.aesKey);
        console.log("signKey   : " + keys.signKey);
        console.log("v4ak      : " + keys.v4ak);
        console.log("v4sk      : " + keys.v4sk);
        console.log("wskey     : " + keys.wskey);
        console.log("tmsk      : " + keys.tmsk);
        console.log("taAppId   : " + keys.taAppId);
        console.log("taSID     : " + keys.taSID);
        console.log("taSK      : " + keys.taSK);

    } catch(e) {
        console.log("[ERROR] 提取 SecBox 密钥失败: " + e);
    }

    // ========== 第二部分：Hook HMAC 签名方法 ==========
    console.log("\n[2] Hook HMAC 签名方法");
    console.log("-".repeat(100));

    try {
        var HmacUtils = Java.use("pf.d");

        // Hook 所有重载版本
        HmacUtils.a.overload('java.lang.String', 'java.lang.String').implementation = function(key, message) {
            console.log("\n" + "*".repeat(100));
            console.log("HMAC-SHA256 签名调用");
            console.log("*".repeat(100));

            console.log("\n【密钥】");
            console.log(key);
            console.log("\n【待签名消息】");
            console.log("长度: " + message.length + " 字节");
            console.log("内容:\n" + message);
            console.log("\n【消息可视化】(用 | 表示 \\n):");
            console.log(message.replace(/\n/g, '\n|\n'));
            console.log("\n【消息十六进制】:");
            var bytes = [];
            for (var i = 0; i < Math.min(message.length, 300); i++) {
                bytes.push(message.charCodeAt(i).toString(16).padStart(2, '0'));
            }
            console.log(bytes.join(' '));
            if (message.length > 300) {
                console.log("... (剩余 " + (message.length - 300) + " 字节)");
            }

            var result = this.a(key, message);

            console.log("\n【签名结果】");
            console.log(result);
            console.log("*".repeat(100) + "\n");

            return result;
        };

        console.log("✓ HMAC 签名方法 Hook 成功");

    } catch(e) {
        console.log("[ERROR] Hook HMAC 方法失败: " + e);
    }

    // ========== 第三部分：Hook 签名拦截器 ==========
    console.log("\n[3] Hook 签名拦截器");
    console.log("-".repeat(100));

    try {
        var MCDSignatureInterceptor = Java.use("qf.e");

        MCDSignatureInterceptor.intercept.implementation = function(chain) {
            console.log("\n" + "=".repeat(100));
            console.log("MCDSignatureInterceptor.intercept() 被调用");
            console.log("=".repeat(100));

            var request = chain.request();
            var url = request.url();
            var method = request.method();
            var headers = request.headers();

            console.log("\n【请求信息】");
            console.log("方法: " + method);
            console.log("URL: " + url.toString());
            console.log("路径: " + url.encodedPath());
            console.log("查询参数: " + url.encodedQuery());

            console.log("\n【原始请求头】");
            var headerNames = headers.names().toArray();
            for (var i = 0; i < headerNames.length; i++) {
                var name = headerNames[i];
                console.log("  " + name + ": " + headers.get(name));
            }

            // 调用原始方法
            var result = this.intercept(chain);

            var newRequest = result.request();
            var newHeaders = newRequest.headers();

            console.log("\n【签名后的请求头】");
            console.log("  Date: " + newHeaders.get("date"));
            console.log("  Authorization: " + newHeaders.get("authorization"));
            console.log("  X-Hmac-Digest: " + newHeaders.get("x-hmac-digest"));

            console.log("=".repeat(100) + "\n");

            return result;
        };

        console.log("✓ 签名拦截器 Hook 成功");

    } catch(e) {
        console.log("[ERROR] Hook 签名拦截器失败: " + e);
    }

    // ========== 第四部分：Hook 字符串拼接工具类 ==========
    console.log("\n[4] Hook 字符串拼接工具");
    console.log("-".repeat(100));

    try {
        // Hook y.J (字符串拼接方法)
        var StringJoiner = Java.use("y");

        StringJoiner.J.implementation = function(separator, parts) {
            var result = this.J(separator, parts);

            // 只打印长度超过 100 的结果（可能是签名消息）
            if (result.length > 100) {
                console.log("\n[字符串拼接] y.J() 被调用");
                console.log("分隔符: " + JSON.stringify(separator));
                console.log("结果长度: " + result.length);
                console.log("结果:\n" + result);
            }

            return result;
        };

        console.log("✓ 字符串拼接工具 Hook 成功");

    } catch(e) {
        console.log("[WARN] Hook 字符串拼接失败 (可能不存在): " + e);
    }

    // ========== 第五部分：Hook Base64 编码 ==========
    console.log("\n[5] Hook Base64 编码");
    console.log("-".repeat(100));

    try {
        var Base64 = Java.use("android.util.Base64");

        Base64.encodeToString.overload('[B', 'int').implementation = function(input, flags) {
            var result = this.encodeToString(input, flags);

            // 只打印签名长度的 Base64（通常 44 字符）
            if (result.length > 30 && result.length < 100) {
                console.log("[Base64 编码] 长度=" + result.length + ", 结果=" + result);
            }

            return result;
        };

        console.log("✓ Base64 编码 Hook 成功");

    } catch(e) {
        console.log("[ERROR] Hook Base64 失败: " + e);
    }

    // ========== 完成 ==========
    console.log("\n" + "=".repeat(100));
    console.log("Hook 安装完成！等待应用发起请求...");
    console.log("请在 APP 中触发菜单请求，所有签名数据将被打印");
    console.log("=".repeat(100) + "\n");
});
