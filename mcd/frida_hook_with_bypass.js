/**
 * Frida 脚本：绕过梆梆反调试 + 提取 SecretKey
 *
 * 使用方法：
 * frida -H 127.0.0.1:9999 -f com.mcdonalds.gma.cn -l frida_hook_with_bypass.js --no-pause
 */

console.log("[*] ============================================================");
console.log("[*] 麦当劳 SecretKey 提取器 + 梆梆反调试绕过");
console.log("[*] ============================================================\n");

// ============ 第一步：绕过反调试 ============

console.log("[*] 第一步：设置反调试绕过...");

// 1. 防止进程被杀死
function preventExit() {
    var libc = null;

    try {
        libc = Process.getModuleByName("libc.so");
    } catch (e) {
        try {
            libc = Process.getModuleByName("libc++.so");
        } catch (e2) {
            console.log("[!] 无法找到 libc，跳过 exit hook");
            return;
        }
    }

    // Hook exit
    try {
        var exitPtr = Module.findExportByName("libc.so", "exit");
        if (exitPtr) {
            Interceptor.replace(exitPtr, new NativeCallback(function(status) {
                console.log("[!] 拦截 exit(" + status + ")，阻止进程退出");
            }, 'void', ['int']));
            console.log("[✅] exit() 已被 Hook");
        }
    } catch (e) {
        console.log("[!] Hook exit 失败: " + e);
    }

    // Hook _exit
    try {
        var _exitPtr = Module.findExportByName("libc.so", "_exit");
        if (_exitPtr) {
            Interceptor.replace(_exitPtr, new NativeCallback(function(status) {
                console.log("[!] 拦截 _exit(" + status + ")，阻止进程退出");
            }, 'void', ['int']));
            console.log("[✅] _exit() 已被 Hook");
        }
    } catch (e) {
        console.log("[!] Hook _exit 失败: " + e);
    }

    // Hook abort
    try {
        var abortPtr = Module.findExportByName("libc.so", "abort");
        if (abortPtr) {
            Interceptor.replace(abortPtr, new NativeCallback(function() {
                console.log("[!] 拦截 abort()，阻止进程崩溃");
            }, 'void', []));
            console.log("[✅] abort() 已被 Hook");
        }
    } catch (e) {
        console.log("[!] Hook abort 失败: " + e);
    }

    // Hook kill (自杀检测)
    try {
        var killPtr = Module.findExportByName("libc.so", "kill");
        if (killPtr) {
            Interceptor.attach(killPtr, {
                onEnter: function(args) {
                    var pid = args[0].toInt32();
                    var sig = args[1].toInt32();
                    if (pid === Process.id || pid === 0) {
                        console.log("[!] 拦截 kill(" + pid + ", " + sig + ")，阻止自杀");
                        args[0] = ptr(-1); // 改为无效 PID
                    }
                }
            });
            console.log("[✅] kill() 已被 Hook");
        }
    } catch (e) {
        console.log("[!] Hook kill 失败: " + e);
    }
}

// 2. 绕过 Frida 检测
function bypassFridaDetection() {
    console.log("[*] 开始绕过 Frida 检测...");

    // 隐藏 Frida 相关字符串
    try {
        var fridaStrings = [
            "frida",
            "FRIDA",
            "Frida",
            "frida-server",
            "frida-agent",
            "re.frida",
            "linjector"
        ];

        fridaStrings.forEach(function(str) {
            Memory.scanSync(Process.enumerateModules()[0].base, Process.enumerateModules()[0].size, str).forEach(function(match) {
                try {
                    Memory.protect(match.address, str.length, 'rw-');
                    match.address.writeByteArray(new Array(str.length).fill(0x00));
                } catch (e) {
                    // 忽略写保护错误
                }
            });
        });

        console.log("[✅] Frida 字符串特征已清理");
    } catch (e) {
        console.log("[!] 清理 Frida 字符串失败: " + e);
    }

    // Hook fopen/fgets (防止读取 maps 检测)
    try {
        var fopenPtr = Module.findExportByName("libc.so", "fopen");
        if (fopenPtr) {
            Interceptor.attach(fopenPtr, {
                onEnter: function(args) {
                    var path = args[0].readCString();
                    if (path && (path.indexOf("/proc") >= 0 && (path.indexOf("maps") >= 0 || path.indexOf("status") >= 0))) {
                        console.log("[!] 拦截 fopen(" + path + ")");
                        // 不返回 NULL，返回 /dev/null 的 FILE*
                        this.path = path;
                        args[0] = Memory.allocUtf8String("/dev/null");
                    }
                }
            });
            console.log("[✅] fopen() 已被 Hook");
        }
    } catch (e) {
        console.log("[!] Hook fopen 失败: " + e);
    }
}

// 3. 反反调试主函数
function antiAntiDebug() {
    preventExit();
    bypassFridaDetection();

    // 延迟一点，让反调试绕过生效
    console.log("[*] 等待 500ms 让绕过生效...");
}

// 立即执行反调试
antiAntiDebug();

// ============ 第二步：Hook HMAC_Init_ex ============

console.log("\n[*] 第二步：等待 libcsiipowerenter.so 加载...");

var targetModule = "libcsiipowerenter.so";
var hmacInitExOffset = 0xEB6E4; // 真实地址

var secretKeyFound = false;

function waitForModule() {
    setTimeout(function() {
        var module = Process.findModuleByName(targetModule);

        if (module === null) {
            console.log("[*] " + targetModule + " 尚未加载，继续等待...");
            waitForModule();
            return;
        }

        console.log("[✅] " + targetModule + " 已加载");
        console.log("[*] 基址: " + module.base);

        var hmacInitExAddr = module.base.add(hmacInitExOffset);
        console.log("[*] HMAC_Init_ex 地址: " + hmacInitExAddr);

        hookHmacInitEx(hmacInitExAddr);
    }, 1000);
}

function hookHmacInitEx(addr) {
    console.log("[*] 开始 Hook HMAC_Init_ex...");

    try {
        Interceptor.attach(addr, {
            onEnter: function(args) {
                console.log("\n" + "=".repeat(60));
                console.log("[✅] HMAC_Init_ex 被调用！");
                console.log("=".repeat(60));

                var keyPtr = args[1];
                var keyLen = args[2].toInt32();

                console.log("[*] Key 指针: " + keyPtr);
                console.log("[*] Key 长度: " + keyLen + " 字节");

                if (keyPtr.isNull() || keyLen <= 0 || keyLen > 256) {
                    console.log("[!] 无效的密钥参数");
                    return;
                }

                try {
                    var keyBytes = keyPtr.readByteArray(keyLen);

                    console.log("\n" + "=".repeat(60));
                    console.log("[🎉] SecretKey 捕获成功！");
                    console.log("=".repeat(60));
                    console.log("[*] 长度: " + keyLen + " 字节");

                    // 输出 HEX
                    var hexString = "";
                    var bytes = new Uint8Array(keyBytes);
                    for (var i = 0; i < bytes.length; i++) {
                        hexString += ("0" + bytes[i].toString(16)).slice(-2);
                    }
                    console.log("[*] HEX: " + hexString);

                    // 输出 Base64
                    var base64 = btoa(String.fromCharCode.apply(null, bytes));
                    console.log("[*] Base64: " + base64);

                    // 尝试 UTF-8
                    try {
                        var utf8String = "";
                        for (var i = 0; i < bytes.length; i++) {
                            utf8String += String.fromCharCode(bytes[i]);
                        }
                        if (isPrintable(utf8String)) {
                            console.log("[*] UTF-8: " + utf8String);
                        }
                    } catch (e) {
                        // 忽略
                    }

                    console.log("=".repeat(60) + "\n");

                    // 输出到文件
                    var timestamp = new Date().toISOString().replace(/[:.]/g, '-').substring(0, 19);
                    var output = "\n麦当劳 SecretKey 提取成功\n";
                    output += "时间: " + new Date().toISOString() + "\n";
                    output += "=" + "=".repeat(59) + "\n";
                    output += "HEX: " + hexString + "\n";
                    output += "Base64: " + base64 + "\n";
                    output += "=" + "=".repeat(59) + "\n\n";
                    output += "请将上述密钥填入 mcdonald_api_auth.py 验证\n";

                    console.log(output);

                    secretKeyFound = true;

                } catch (e) {
                    console.log("[!] 读取密钥失败: " + e);
                }
            },

            onLeave: function(retval) {
                // console.log("[*] HMAC_Init_ex 返回值: " + retval);
            }
        });

        console.log("[✅] Hook 设置成功！");
        console.log("[*] 等待 HMAC_Init_ex 被调用...");
        console.log("[*] 请在 App 中触发登录或 API 请求\n");

    } catch (e) {
        console.log("[!] Hook 失败: " + e);
    }
}

function isPrintable(str) {
    for (var i = 0; i < str.length; i++) {
        var code = str.charCodeAt(i);
        if (code < 32 || code > 126) {
            return false;
        }
    }
    return true;
}

function btoa(str) {
    var keyStr = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
    var output = "";
    var chr1, chr2, chr3, enc1, enc2, enc3, enc4;
    var i = 0;

    while (i < str.length) {
        chr1 = str.charCodeAt(i++);
        chr2 = str.charCodeAt(i++);
        chr3 = str.charCodeAt(i++);

        enc1 = chr1 >> 2;
        enc2 = ((chr1 & 3) << 4) | (chr2 >> 4);
        enc3 = ((chr2 & 15) << 2) | (chr3 >> 6);
        enc4 = chr3 & 63;

        if (isNaN(chr2)) {
            enc3 = enc4 = 64;
        } else if (isNaN(chr3)) {
            enc4 = 64;
        }

        output += keyStr.charAt(enc1) + keyStr.charAt(enc2) +
                  keyStr.charAt(enc3) + keyStr.charAt(enc4);
    }

    return output;
}

// 启动模块监控
waitForModule();

console.log("[*] 脚本初始化完成");
console.log("[*] 如果 App 崩溃，请尝试:");
console.log("    1. 重启 Frida Server");
console.log("    2. 使用 spawn 模式: frida -H 127.0.0.1:9999 -f com.mcdonalds.gma.cn -l <script> --no-pause");
console.log("    3. 或者尝试 attach 到已运行的进程\n");
