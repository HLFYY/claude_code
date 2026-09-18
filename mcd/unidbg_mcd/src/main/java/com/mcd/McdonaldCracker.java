package com.mcd;

import com.github.unidbg.AndroidEmulator;
import com.github.unidbg.Module;
import com.github.unidbg.linux.android.AndroidEmulatorBuilder;
import com.github.unidbg.linux.android.AndroidResolver;
import com.github.unidbg.linux.android.dvm.*;
import com.github.unidbg.memory.Memory;
import com.github.unidbg.hook.hookzz.*;
import com.github.unidbg.arm.backend.Backend;
import com.github.unidbg.pointer.UnidbgPointer;

import java.io.File;
import java.util.Arrays;

/**
 * 麦当劳 SecretKey 提取器
 * 使用 unidbg 模拟执行 libcsiipowerenter.so，Hook HMAC_Init_ex 提取密钥
 */
public class McdonaldCracker extends AbstractJni {

    private final AndroidEmulator emulator;
    private final VM vm;
    private final Module module;

    private byte[] secretKey = null;
    private boolean keyFound = false;

    public McdonaldCracker() {
        // 1. 创建 ARM64 模拟器
        emulator = AndroidEmulatorBuilder
                .for64Bit()
                .setProcessName("com.mcdonalds.gma.cn")
                .build();

        // 2. 获取内存
        Memory memory = emulator.getMemory();
        memory.setLibraryResolver(new AndroidResolver(23)); // Android 6.0

        // 3. 创建 Dalvik VM
        vm = emulator.createDalvikVM();
        vm.setVerbose(true);
        vm.setJni(this);

        // 4. 加载 SO 库
        System.out.println("[*] 加载 libcsiipowerenter.so...");

        // 根据实际路径调整
        String soPath = "so_libs/libcsiipowerenter.so";
        File soFile = new File(soPath);

        if (!soFile.exists()) {
            System.err.println("[!] SO 文件不存在: " + soPath);
            System.err.println("[!] 请将 libcsiipowerenter.so 放到 so_libs/ 目录");
            System.exit(1);
        }

        DalvikModule dm = vm.loadLibrary(soFile, false);
        module = dm.getModule();

        System.out.println("[*] SO 加载成功，基址: " + Long.toHexString(module.base));

        // 5. 设置 Hook
        setupHooks();

        // 6. 调用 JNI_OnLoad（如果需要）
        dm.callJNI_OnLoad(emulator);
    }

    /**
     * 设置 HMAC_Init_ex Hook
     */
    private void setupHooks() {
        System.out.println("[*] 设置 HMAC_Init_ex Hook...");

        // HMAC_Init_ex 的偏移地址（从 IDA 分析得到）
        long hmacInitExOffset = 0x2127A;
        long hmacInitExAddr = module.base + hmacInitExOffset;

        System.out.println("[*] HMAC_Init_ex 地址: 0x" + Long.toHexString(hmacInitExAddr));

        IHookZz hookZz = HookZz.getInstance(emulator);

        hookZz.wrap(hmacInitExAddr, new WrapCallback<HookZzArm64RegisterContext>() {
            @Override
            public void preCall(Emulator<?> emulator, HookZzArm64RegisterContext ctx, HookEntryInfo info) {
                /*
                 * ARM64 调用约定:
                 * int HMAC_Init_ex(
                 *     HMAC_CTX *ctx,      // X0
                 *     const void *key,    // X1 ← 目标！
                 *     int len,            // X2
                 *     const EVP_MD *md,   // X3
                 *     ENGINE *impl        // X4
                 * );
                 */

                System.out.println("\n[*] ========================================");
                System.out.println("[*] HMAC_Init_ex 被调用！");

                // 读取密钥指针和长度
                long keyPtr = ctx.getXLong(1);
                int keyLen = ctx.getXInt(2);

                System.out.println("[*] 密钥指针: 0x" + Long.toHexString(keyPtr));
                System.out.println("[*] 密钥长度: " + keyLen + " 字节");

                if (keyPtr != 0 && keyLen > 0 && keyLen <= 256) {
                    try {
                        // 从内存读取密钥
                        Backend backend = emulator.getBackend();
                        byte[] key = backend.mem_read(keyPtr, keyLen);

                        // 保存密钥
                        secretKey = key;
                        keyFound = true;

                        // 输出密钥
                        System.out.println("[*] ========================================");
                        System.out.println("[✅] SecretKey 捕获成功！");
                        System.out.println("[*] 长度: " + key.length + " 字节");
                        System.out.println("[*] HEX: " + bytesToHex(key));
                        System.out.println("[*] Base64: " + java.util.Base64.getEncoder().encodeToString(key));

                        // 尝试解析为 UTF-8 字符串
                        try {
                            String keyStr = new String(key, "UTF-8");
                            if (isPrintable(keyStr)) {
                                System.out.println("[*] UTF-8: " + keyStr);
                            }
                        } catch (Exception e) {
                            // 忽略非 UTF-8 数据
                        }

                        System.out.println("[*] ========================================\n");

                    } catch (Exception e) {
                        System.err.println("[!] 读取密钥失败: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }

            @Override
            public void postCall(Emulator<?> emulator, HookZzArm64RegisterContext ctx, HookEntryInfo info) {
                // 可以在这里读取返回值
                int result = ctx.getXInt(0);
                System.out.println("[*] HMAC_Init_ex 返回值: " + result);
            }
        });

        System.out.println("[*] Hook 设置完成");
    }

    /**
     * 调用 csiiEncrypt JNI 函数
     */
    public void callCsiiEncrypt() {
        System.out.println("\n[*] 调用 csiiEncrypt...");

        // 准备测试数据
        String testData = "test data for encryption";
        byte[] inputBytes = testData.getBytes();

        try {
            // 调用 JNI 函数
            // 签名: ([B)[B - 输入 byte[]，返回 byte[]
            DvmObject<?> result = vm.getObject(
                module.callFunction(
                    emulator,
                    "Java_com_mcdonalds_app_NativeLib_csiiEncrypt",
                    vm.getJNIEnv(),
                    0, // jobject (可以为 0)
                    vm.addLocalObject(new ByteArray(vm, inputBytes))
                )[0]
            );

            if (result != null) {
                System.out.println("[*] csiiEncrypt 调用成功");
            }

        } catch (Exception e) {
            System.err.println("[!] csiiEncrypt 调用失败: " + e.getMessage());

            // 尝试使用简化的调用方式
            System.out.println("[*] 尝试直接调用导出函数...");

            try {
                // 查找导出的 csiiEncrypt
                long csiiEncryptAddr = module.findSymbolByName("csiiEncrypt").getAddress();
                System.out.println("[*] csiiEncrypt 地址: 0x" + Long.toHexString(csiiEncryptAddr));

                // 直接调用（可能需要调整参数）
                emulator.getBackend().reg_write(com.github.unidbg.arm.backend.Unicorn2.UC_ARM64_REG_X0,
                    vm.getJNIEnv());
                emulator.getBackend().reg_write(com.github.unidbg.arm.backend.Unicorn2.UC_ARM64_REG_X1, 0);

                // 分配内存存储输入数据
                UnidbgPointer inputPtr = memory.malloc(inputBytes.length, false).getPointer();
                inputPtr.write(0, inputBytes, 0, inputBytes.length);

                emulator.getBackend().reg_write(com.github.unidbg.arm.backend.Unicorn2.UC_ARM64_REG_X2,
                    inputPtr.peer);

                module.callFunction(emulator, csiiEncryptAddr);

            } catch (Exception e2) {
                System.err.println("[!] 直接调用也失败: " + e2.getMessage());
            }
        }
    }

    /**
     * 获取提取的密钥
     */
    public byte[] getSecretKey() {
        return secretKey;
    }

    /**
     * 是否成功提取密钥
     */
    public boolean isKeyFound() {
        return keyFound;
    }

    /**
     * 关闭模拟器
     */
    public void destroy() {
        if (emulator != null) {
            emulator.close();
        }
    }

    // ============ 辅助方法 ============

    /**
     * 字节数组转十六进制字符串
     */
    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /**
     * 判断字符串是否可打印
     */
    private static boolean isPrintable(String str) {
        for (char c : str.toCharArray()) {
            if (c < 32 || c > 126) {
                return false;
            }
        }
        return true;
    }

    // ============ 主函数 ============

    public static void main(String[] args) {
        System.out.println("============================================================");
        System.out.println("麦当劳 SecretKey 提取器 - unidbg 方案");
        System.out.println("============================================================\n");

        McdonaldCracker cracker = null;

        try {
            // 创建实例（会自动加载 SO 和设置 Hook）
            cracker = new McdonaldCracker();

            // 调用加密函数（触发 Hook）
            cracker.callCsiiEncrypt();

            // 检查结果
            Thread.sleep(1000); // 等待 Hook 执行

            if (cracker.isKeyFound()) {
                byte[] key = cracker.getSecretKey();

                System.out.println("\n============================================================");
                System.out.println("[✅] 任务完成！SecretKey 已成功提取");
                System.out.println("============================================================");
                System.out.println("密钥 (HEX): " + bytesToHex(key));
                System.out.println("密钥 (Base64): " + java.util.Base64.getEncoder().encodeToString(key));
                System.out.println("============================================================");
                System.out.println("\n请将上述密钥填入 mcdonald_api_auth.py 进行验证");

            } else {
                System.err.println("\n[❌] 未能提取到密钥");
                System.err.println("可能的原因:");
                System.err.println("1. csiiEncrypt 函数未正确调用");
                System.err.println("2. 函数内部未调用 HMAC_Init_ex");
                System.err.println("3. Hook 地址不正确");
                System.err.println("\n建议:");
                System.err.println("1. 检查 SO 库是否完整");
                System.err.println("2. 确认 HMAC_Init_ex 偏移地址 (当前: 0x2127A)");
                System.err.println("3. 查看详细的调试日志");
            }

        } catch (Exception e) {
            System.err.println("\n[!] 发生错误: " + e.getMessage());
            e.printStackTrace();

        } finally {
            if (cracker != null) {
                cracker.destroy();
            }
        }
    }
}
