/**
 * Frida Script: Dump 内存中已解密的 libcsiipowerenter.so
 *
 * 使用方法:
 * frida -U -f com.mcdonalds.gma.cn -l frida_dump_so.js --no-pause
 */

console.log("[*] ============================================================");
console.log("[*] Dump 解密后的 libcsiipowerenter.so 到文件");
console.log("[*] ============================================================\n");

var targetModule = "libcsiipowerenter.so";
var dumpPath = "/sdcard/libcsiipowerenter_dumped.so";

function waitForModule() {
    setTimeout(function() {
        var module = Process.findModuleByName(targetModule);

        if (module === null) {
            console.log("[*] 等待 " + targetModule + " 加载...");
            waitForModule();
            return;
        }

        console.log("[✅] " + targetModule + " 已加载");
        console.log("[*] 基址: " + module.base);
        console.log("[*] 大小: " + module.size + " bytes (" + (module.size / 1024 / 1024).toFixed(2) + " MB)");
        console.log("[*] 路径: " + module.path);

        dumpModule(module);
    }, 1000);
}

function dumpModule(module) {
    console.log("\n[*] 开始 dump 内存...");

    try {
        var baseAddr = module.base;
        var size = module.size;

        // 读取整个模块的内存
        var buffer = Memory.readByteArray(baseAddr, size);

        console.log("[✅] 内存读取成功");
        console.log("[*] 正在写入文件: " + dumpPath);

        // 写入文件
        var file = new File(dumpPath, "wb");
        file.write(buffer);
        file.close();

        console.log("[✅] Dump 完成！");
        console.log("\n" + "=".repeat(60));
        console.log("[🎉] 成功 dump 到: " + dumpPath);
        console.log("=".repeat(60));
        console.log("\n[*] 请使用 adb pull 拉取文件:");
        console.log("    adb pull " + dumpPath + " ~/Desktop/libcsiipowerenter_dumped.so");
        console.log("\n[*] 然后用 IDA Pro 分析 dump 出的文件");

        // 额外：尝试直接搜索内存中的 32 字节密钥特征
        searchSecretKey(baseAddr, size);

    } catch (e) {
        console.log("[!] Dump 失败: " + e);
    }
}

function searchSecretKey(baseAddr, size) {
    console.log("\n[*] 同时在内存中搜索可能的 SecretKey...");

    // 搜索策略：找到 HMAC_Init_ex 附近的数据区域
    var hmacInitExOffset = 0xEB6E4;
    var targetAddr = baseAddr.add(hmacInitExOffset);

    console.log("[*] HMAC_Init_ex @ " + targetAddr);
    console.log("[*] 查看附近的内存数据...\n");

    // 显示目标地址附近的内存
    console.log("=== 内存 Dump (HMAC_Init_ex) ===");
    console.log(hexdump(targetAddr, {
        offset: 0,
        length: 256,
        header: true,
        ansi: false
    }));

    // 搜索已知的加密数据块区域
    var encryptedDataOffset = 0x6AD58;
    var encryptedDataAddr = baseAddr.add(encryptedDataOffset);

    console.log("\n=== 内存 Dump (加密数据区) ===");
    console.log(hexdump(encryptedDataAddr, {
        offset: 0,
        length: 256,
        header: true,
        ansi: false
    }));

    console.log("\n[*] 提示：如果看到正常的 ARM64 指令而不是 DCQ 数据，说明代码已解密");
    console.log("[*] 如果仍然看到 DCQ 数据，可能需要触发一次加密操作后再 dump");
}

console.log("[*] 初始化完成，开始监控...\n");
waitForModule();
