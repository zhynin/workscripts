#!/bin/bash
set -euo pipefail

# 全局配置
COCKROACH_VERSION="25.2.2"
INSTALL_DIR="/usr/local/cockroachdb"
DATA_DIR="/opt/cockroachdb"
CERT_DIR="$DATA_DIR/certs"

# 颜色输出定义
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'


# --------------------------
# 显示用法
# --------------------------
usage() {
    echo "用法: $0 [选项]"
    echo "选项:"
    echo "  --install     安装 CockroachDB"
    echo "  --uninstall   卸载 CockroachDB"
    echo "  --start       启动 CockroachDB"
    echo "  --stop        停止 CockroachDB"
    echo "  --status      查看 CockroachDB"
    echo "  -h, --help    显示帮助信息"
    exit 1
}

get_local_ip() {
    local ip=""
    # 尝试通过默认路由获取
    ip=$(ip route get 1.1.1.1 2>/dev/null | awk '{print $7}' | head -1)
    
    # 回退方案：遍历所有非回环IP
    if [ -z "$ip" ]; then
        ip=$(ip -br -4 addr show | grep -v "127.0.0.1" | awk '{print $3}' | cut -d'/' -f1 | head -1)
    fi
    
    echo "${ip:-N/A}"
}

LOCAL_IP=$(get_local_ip)


# --------------------------
# 检查依赖：glibc ncurses tzdata
# --------------------------
check_dependencies() {
    echo -e "${GREEN}[1/5] 校验系统依赖...${NC}"
    local missing=()

    # 检查依赖项
    command -v curl >/dev/null 2>&1 || { echo -e "${RED}请先安装 curl${NC}"; exit 1; }
    command -v sha256sum >/dev/null 2>&1 || { echo -e "${RED}请先安装 sha256sum${NC}"; exit 1; }

    # 检查 glibc (通过 ldd 存在性间接检查)
    if ! ldd --version &>/dev/null; then
        missing+=("glibc")
    fi

    # 检查 libncurses (通过查找库文件)
    if ! ldconfig -p | grep -q libncurses.so; then
        missing+=("libncurses")
    fi

    # 检查 tzdata (通过检查时区文件)
    if [ ! -f /usr/share/zoneinfo/UTC ] && [ ! -f /etc/localtime ]; then
        missing+=("tzdata")
    fi

    # 如果缺少依赖则报错
    if [ ${#missing[@]} -gt 0 ]; then
        echo -e "${RED}错误: 缺少以下依赖库: ${missing[*]}${NC}"
        echo -e "请根据系统类型安装:"
        echo -e "  Debian/Ubuntu: sudo apt install libc6 libncurses6 tzdata"
        echo -e "  RHEL/CentOS:   sudo yum install glibc ncurses tzdata"
        exit 1
    fi

    echo -e "${GREEN}依赖 glibc, libncurses 和 tzdata 已满足！${NC}"
}

# --------------------------
# 下载和校验安装包
# --------------------------
download_and_verify() {
    local pkg_name="cockroach-v${COCKROACH_VERSION}.linux-amd64.tgz"
    local url="https://binaries.cockroachdb.com/${pkg_name}"
    local sha256_url="${url}.sha256sum"

    echo -e "${GREEN}[2/5] 使用 curl 下载并校验...${NC}"

    #下载安装包 (显示进度条)
    # if ! curl -# -L -o "$pkg_name" "$url"; then
    #     echo -e "${RED}错误: 下载安装包失败${NC}"
    #     exit 1
    # fi

    # 下载校验文件 (静默模式)
    if ! curl -s -L -o "${pkg_name}.sha256sum" "$sha256_url"; then
        echo -e "${RED}错误: 下载校验文件失败${NC}"
        exit 1
    fi

    # 校验SHA256 (兼容 curl 下载的文件)
    if ! sha256sum -c "${pkg_name}.sha256sum"; then
        echo -e "${RED}错误: 安装包校验失败${NC}"
        exit 1
    fi

    echo -e "${GREEN}校验通过！${NC}"
}

# --------------------------
# 解压和安装
# --------------------------
install_cockroachdb() {
    local pkg_name="cockroach-v${COCKROACH_VERSION}.linux-amd64.tgz"

    echo -e "${GREEN}[3/5] 安装 CockroachDB...${NC}"

    # 解压到临时目录
    temp_dir=$(mktemp -d)
    tar xzf "$pkg_name" -C "$temp_dir" --strip-components=1

    # 创建安装目录
    sudo mkdir -p "$INSTALL_DIR"
    sudo cp "$temp_dir/cockroach" "$INSTALL_DIR/"
    sudo cp -r "$temp_dir/lib" "$INSTALL_DIR/"

    # 添加到系统PATH
    sudo ln -sf "$INSTALL_DIR/cockroach" /usr/local/bin/cockroach
    sudo ln -sf "$INSTALL_DIR/lib/libgeos_c.so" /usr/local/lib/libgeos_c.so
    sudo ln -sf "$INSTALL_DIR/lib/libgeos.so" /usr/local/lib/libgeos.so

    # 清理临时文件
    rm -rf "$temp_dir" #"$pkg_name"*

    echo -e "${GREEN}安装完成！版本信息:${NC}"
    cockroach version
}

# --------------------------
# 生成证书
# --------------------------
generate_certs() {
    echo -e "${GREEN}[4/5] 生成证书...${NC}"

    # 创建证书目录
    sudo mkdir -p "$CERT_DIR"
    sudo chown -R "$(whoami)" "$CERT_DIR"

    # 生成CA证书
    cockroach cert create-ca \
        --certs-dir="$CERT_DIR" \
        --ca-key="$CERT_DIR/ca.key"

    # 生成节点证书
    local hostname=$(hostname)
    cockroach cert create-node \
        "$hostname" \
        "$LOCAL_IP" \
        "localhost" \
        "127.0.0.1" \
        --certs-dir="$CERT_DIR" \
        --ca-key="$CERT_DIR/ca.key"

    # 生成客户端证书（可选）
    cockroach cert create-client \
        root \
        --certs-dir="$CERT_DIR" \
        --ca-key="$CERT_DIR/ca.key"

    echo -e "${GREEN}证书生成完成！${NC}"
}

# --------------------------
# 初始化实例
# --------------------------
initialize_instance() {
    echo -e "${GREEN}[5/5] 初始化实例...${NC}"

    # 创建数据目录
    sudo mkdir -p "$DATA_DIR"
    sudo chown -R "$(whoami)" "$DATA_DIR"

    # 初始化集群
    cockroach_start
    cockroach init --cluster-name="zhynin" --certs-dir=$CERT_DIR --host=$LOCAL_IP:26257

    echo -e "${GREEN}实例已启动！${NC}"
    echo -e "使用以下命令连接:"
    # echo -e "  cockroach sql --certs-dir=$CERT_DIR --user=root"
    # grep 'node starting' $DATA_DIR/node1/logs/cockroach.log -A 11
    echo -e "  cockroach sql --certs-dir=$CERT_DIR --user=root --host=$LOCAL_IP:20157"
}


# --------------------------
# 模块: 启动 cockroach
# --------------------------
cockroach_start() {
    cockroach start \
        --cluster-name="zhynin" \
        --certs-dir="$CERT_DIR" \
        --store="attrs=ssd,path=$DATA_DIR/node1" \
        --sql-addr=:20157 \
        --listen-addr=:26257 \
        --http-addr=:8080 \
        --cache=.25 \
        --max-sql-memory=.25 \
        --advertise-addr=${LOCAL_IP} \
        --advertise-sql-addr=${LOCAL_IP} \
        --join=${LOCAL_IP}:26257,${LOCAL_IP}:26258,${LOCAL_IP}:26259 \
        --background

    cockroach start \
        --cluster-name="zhynin" \
        --certs-dir="$CERT_DIR" \
        --store="attrs=ssd,path=$DATA_DIR/node2" \
        --sql-addr=:20158 \
        --listen-addr=:26258 \
        --http-addr=:8081 \
        --cache=.25 \
        --max-sql-memory=.25 \
        --advertise-addr=${LOCAL_IP} \
        --advertise-sql-addr=${LOCAL_IP} \
        --join=${LOCAL_IP}:26257,${LOCAL_IP}:26258,${LOCAL_IP}:26259 \
        --background

    cockroach start \
        --cluster-name="zhynin" \
        --certs-dir="$CERT_DIR" \
        --store="attrs=ssd,path=$DATA_DIR/node3" \
        --sql-addr=:20169 \
        --listen-addr=:26259 \
        --http-addr=:8082 \
        --cache=.25 \
        --max-sql-memory=.25 \
        --advertise-addr=${LOCAL_IP} \
        --advertise-sql-addr=${LOCAL_IP} \
        --join=${LOCAL_IP}:26257,${LOCAL_IP}:26258,${LOCAL_IP}:26259 \
        --background
}

cockroach_stop() {
    # 停止运行中的实例
    # if pgrep -x "cockroach" > /dev/null; then
    #     echo "停止运行中的实例..."
    #     pkill -x "cockroach"
    #     sleep 3
    # fi
    ps -ef | grep "cockroach start" | grep -v grep | awk '{print $2}' | xargs -r kill -9
    sleep 3
}

cockroach_status() {
    cockroach node status --certs-dir="$CERT_DIR"
}

# --------------------------
# 卸载 CockroachDB
# --------------------------
uninstall_cockroachdb() {
    echo -e "${RED}开始卸载 CockroachDB...${NC}"

    # 停止运行中的实例
    #cockroach_stop
    if pgrep -x "cockroach" > /dev/null; then
        echo "停止运行中的实例..."
        pkill -x "cockroach"
        sleep 3
    fi

    # 删除安装文件
    echo "删除安装目录..."
    sudo rm -rf "$INSTALL_DIR"
    sudo rm -f /usr/local/bin/cockroach
    sudo rm -f /usr/local/lib/libgeos_c.so
    sudo rm -f /usr/local/lib/libgeos.so

    # 可选：删除数据目录（默认保留）
    read -p "是否删除数据目录 $DATA_DIR？[y/N] " choice
    case "$choice" in
        y|Y) sudo rm -rf "$DATA_DIR";;
        *) echo "保留数据目录 $DATA_DIR";;
    esac

    echo -e "${GREEN}卸载完成！${NC}"
}



# --------------------------
# 主执行流程
# --------------------------
main() {
    case "$1" in
        --install)
            echo -e "${GREEN}=== 安装 CockroachDB ===${NC}"
            check_dependencies
            download_and_verify
            install_cockroachdb
            generate_certs
            initialize_instance
            ;;
        --uninstall)
            echo -e "${RED}=== 卸载 CockroachDB ===${NC}"
            uninstall_cockroachdb
            ;;
        --start)
            echo -e "${GREEN}=== 启动 CockroachDB ===${NC}"
            cockroach_start
            ;;
        --stop)
            echo -e "${RED}=== 停止 CockroachDB ===${NC}"
            cockroach_stop
            ;;
        --status)
            echo -e "${GREEN}=== 查看 CockroachDB ===${NC}"
            cockroach_status
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo -e "${RED}错误: 未知参数 '$1'${NC}"
            usage
            ;;
    esac
}

# 参数检查
if [ $# -eq 0 ]; then
    usage
else
    main "$1"
fi