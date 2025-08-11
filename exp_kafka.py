import argparse
import requests
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
import time

# ======================
# 1. 解析命令行参数
# ======================
def parse_args():
    parser = argparse.ArgumentParser(description="检测 Kafka 未授权访问及高危操作（支持自定义 IP 和端口，带默认值）")
    parser.add_argument(
        "--kafka-ip", "-k",
        type=str,
        required=True,
        help="Kafka Broker 的 IP 地址，例如：192.168.193.48"
    )
    parser.add_argument(
        "--kafka-port", "-kp",
        type=int,
        default=9092,
        help="Kafka Broker 的端口，默认：9092"
    )
    parser.add_argument(
        "--web-ip", "-w",
        type=str,
        required=False,
        default=None,
        help="（可选）Kafka Web 管理界面的 IP 地址，默认与 Kafka IP 相同"
    )
    parser.add_argument(
        "--web-port", "-wp",
        type=int,
        default=9090,
        help="（可选）Kafka Web 管理界面的端口，默认：9090"
    )
    return parser.parse_args()

# ======================
# 2. 主检测逻辑
# ======================
def main():
    args = parse_args()

    KAFKA_IP = args.kafka_ip
    KAFKA_PORT = args.kafka_port
    WEB_PORT = args.web_port

    # 如果未单独指定 Web IP，则默认与 Kafka IP 相同
    WEB_IP = args.web_ip if args.web_ip else KAFKA_IP

    # 组装地址
    KAFKA_BROKER = f"{KAFKA_IP}:{KAFKA_PORT}"                     # 如 192.168.193.48:9092
    WEB_MANAGEMENT_UI = f"http://{WEB_IP}:{WEB_PORT}"             # 如 http://192.168.193.48:9090

    print(f"[🔐] 开始检测 Kafka 服务...")
    print(f"    Kafka Broker: {KAFKA_BROKER}")
    print(f"    Web 管理界面: {WEB_MANAGEMENT_UI}")

    # ======================
    # 3. 功能 1：列出 Topics（检测未授权访问）
    # ======================
    print("\n🔍 [1/4] 检测：是否能列出 Topics（基础未授权访问）...")
    try:
        from kafka import KafkaAdminClient
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        topics = admin_client.list_topics()
        print(f"[+] ✅ 可访问 Topics！列表如下：")
        for t in topics:
            print(f"   - {t}")

        global TEST_TOPIC
        if topics:
            TEST_TOPIC = topics[0]
            print(f"[*] 将使用第一个 Topic 进行读写测试: {TEST_TOPIC}")
        else:
            print(f"[!] ⚠️ 未发现任何 Topic")
        success_kafka = True
    except Exception as e:
        print(f"[-] ❌ 无法列出 Topics，可能被拒绝或需要认证: {e}")
        success_kafka = False

    # ======================
    # 4. 功能 2 & 3：消费 & 生产消息（如果 Kafka 可访问）
    # ======================
    def test_consume():
        if not TEST_TOPIC:
            print("[!] ⚠️ 未获取到 Topic，跳过消费测试")
            return False
        try:
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(
                TEST_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset='earliest',
                consumer_timeout_ms=5000
            )
            msgs = list(consumer)
            if msgs:
                print(f"[+] ✅ 可消费消息！共 {len(msgs)} 条，展示前 3 条：")
                for i, msg in enumerate(msgs[:3]):
                    try:
                        print(f"   [消息{i+1}] {msg.value.decode('utf-8', errors='replace')}")
                    except:
                        print(f"   [消息{i+1}] [二进制/无法解码]")
            else:
                print(f"[!] ⚠️ 可连接 Topic，但无消息")
            return True
        except Exception as e:
            print(f"[-] ❌ 无法消费消息: {e}")
            return False

    def test_produce():
        if not TEST_TOPIC:
            print("[!] ⚠️ 未获取到 Topic，跳过生产测试")
            return False
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
            test_id = int(time.time())
            msg = f"Kafka未授权测试消息 {test_id}".encode('utf-8')
            future = producer.send(TEST_TOPIC, msg)
            meta = future.get(timeout=5)
            print(f"[+] ✅ 可生产消息！写入 Topic: {meta.topic} [Partition: {meta.partition}]")
            return True
        except Exception as e:
            print(f"[-] ❌ 无法生产消息: {e}")
            return False

    if success_kafka:
        test_consume()
        test_produce()
    else:
        print("[!] 由于无法列出 Topics，跳过消费/生产测试")

    # ======================
    # 5. 功能 4：访问 Web 管理界面
    # ======================
    print("\n🔍 [4/4] 检测：是否能访问 Web 管理界面（无登录）...")
    try:
        response = requests.get(WEB_MANAGEMENT_UI, timeout=5)
        if response.status_code == 200:
            print(f"[+] ✅ 可访问管理界面！（无认证，状态码 200）")
            print(f"    📌 URL: {WEB_MANAGEMENT_UI}")
            print("    ⚠️  可能暴露 Topic / 消费者组等敏感数据")
        else:
            print(f"[-] ❌ 管理界面返回状态码: {response.status_code}，可能不存在或需认证")
    except Exception as e:
        print(f"[-] ❌ 无法访问管理界面，可能未开放或网络不通: {e}")

# ======================
# 6. 入口
# ======================
if __name__ == '__main__':
    main()
