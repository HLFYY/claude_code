#!/bin/bash

# 环境检查脚本

echo "============================================================"
echo "unidbg 环境检查"
echo "============================================================"
echo ""

# 1. 检查 Java
echo "[1] 检查 Java..."
if command -v java &> /dev/null; then
    java -version
    JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
    if [ "$JAVA_VERSION" -ge 17 ]; then
        echo "✅ Java 版本满足要求 (>= 17)"
    else
        echo "❌ Java 版本过低，需要 17+"
        echo "   安装: brew install openjdk@17"
    fi
else
    echo "❌ 未找到 Java"
    echo "   安装: brew install openjdk@17"
fi

echo ""

# 2. 检查 Maven
echo "[2] 检查 Maven..."
if command -v mvn &> /dev/null; then
    mvn -version | head -n 1
    echo "✅ Maven 已安装"
else
    echo "❌ 未找到 Maven"
    echo "   安装: brew install maven"
fi

echo ""

# 3. 检查 SO 文件
echo "[3] 检查 SO 文件..."
if [ -f "libs/arm64-v8a/libcsiipowerenter.so" ]; then
    SO_SIZE=$(du -h libs/arm64-v8a/libcsiipowerenter.so | cut -f1)
    echo "✅ SO 文件存在 (大小: $SO_SIZE)"
else
    echo "❌ SO 文件不存在"
    echo "   位置: libs/arm64-v8a/libcsiipowerenter.so"
fi

echo ""

# 4. 检查项目结构
echo "[4] 检查项目结构..."
if [ -f "pom.xml" ]; then
    echo "✅ pom.xml 存在"
else
    echo "❌ pom.xml 不存在"
fi

if [ -f "src/main/java/com/mcd/McdonaldCracker.java" ]; then
    echo "✅ 主类文件存在"
else
    echo "❌ 主类文件不存在"
fi

echo ""
echo "============================================================"
echo "检查完成"
echo "============================================================"
echo ""
echo "如果所有检查都通过，运行: ./run.sh"
