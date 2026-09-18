#!/bin/bash

# 不使用 Maven 的简化运行脚本

set -e

echo "============================================================"
echo "麦当劳 SecretKey 提取器 - 简化运行方案"
echo "============================================================"
echo ""

cd "$(dirname "$0")"

# 1. 下载 unidbg JAR
echo "[*] 检查 unidbg 依赖..."
UNIDBG_JAR="unidbg-android-0.9.7.jar"

if [ ! -f "$UNIDBG_JAR" ]; then
    echo "[*] 下载 unidbg (这可能需要几分钟)..."
    curl -L -o "$UNIDBG_JAR" \
        "https://repo1.maven.org/maven2/com/github/zhkl0228/unidbg-android/0.9.7/unidbg-android-0.9.7.jar"

    if [ $? -ne 0 ]; then
        echo "[!] 下载失败，请手动下载："
        echo "    https://repo1.maven.org/maven2/com/github/zhkl0228/unidbg-android/0.9.7/unidbg-android-0.9.7.jar"
        exit 1
    fi
fi

echo "[✅] unidbg 就绪"
echo ""

# 2. 编译 Java 代码
echo "[*] 编译 Java 代码..."
javac -cp "$UNIDBG_JAR" -d build src/main/java/com/mcd/McdonaldCracker.java

if [ $? -ne 0 ]; then
    echo "[!] 编译失败"
    exit 1
fi

echo "[✅] 编译成功"
echo ""

# 3. 运行
echo "[*] 启动 unidbg 模拟器..."
echo ""
echo "============================================================"
echo ""

java -cp "$UNIDBG_JAR:build" com.mcd.McdonaldCracker

echo ""
echo "============================================================"
echo "[*] 程序运行完成"
echo "============================================================"
