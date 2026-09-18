#!/bin/bash

# 麦当劳 SecretKey 提取器 - 运行脚本

set -e

echo "============================================================"
echo "麦当劳 SecretKey 提取器 - unidbg 方案"
echo "============================================================"
echo ""

# 检查 Java 版本
echo "[*] 检查 Java 环境..."
if ! command -v java &> /dev/null; then
    echo "[!] 未找到 Java，请先安装 Java 17+"
    echo "    macOS: brew install openjdk@17"
    exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
if [ "$JAVA_VERSION" -lt 17 ]; then
    echo "[!] Java 版本过低（当前: $JAVA_VERSION），需要 Java 17+"
    exit 1
fi

echo "[✅] Java 版本: $JAVA_VERSION"
echo ""

# 检查 Maven
echo "[*] 检查 Maven..."
if ! command -v mvn &> /dev/null; then
    echo "[!] 未找到 Maven，请先安装 Maven"
    echo "    macOS: brew install maven"
    exit 1
fi

echo "[✅] Maven 已安装"
echo ""

# 进入项目目录
cd "$(dirname "$0")"

# 检查 SO 文件
if [ ! -f "libs/arm64-v8a/libcsiipowerenter.so" ]; then
    echo "[!] 未找到 SO 文件: libs/arm64-v8a/libcsiipowerenter.so"
    exit 1
fi

echo "[✅] SO 文件就绪"
echo ""

# 编译项目
echo "[*] 编译项目..."
mvn clean package -DskipTests

if [ $? -ne 0 ]; then
    echo "[!] 编译失败"
    exit 1
fi

echo ""
echo "[✅] 编译成功"
echo ""

# 运行
echo "[*] 启动 unidbg 模拟器..."
echo ""
echo "============================================================"
echo ""

java -jar target/mcdonald-cracker-1.0-SNAPSHOT.jar

echo ""
echo "============================================================"
echo "[*] 程序运行完成"
echo "============================================================"
