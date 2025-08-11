✅ 参数说明
参数	是否必填	默认值	示例	说明
--kafka-ip / -k	✅ 必填	无	192.168.193.48	Kafka Broker 的 IP 地址
--kafka-port / -kp	❌ 可选	9092	9093	Kafka 端口
--web-ip / -w	❌ 可选	与 Kafka IP 相同	192.168.193.49	Web UI 的 IP（可选，默认同 Kafka IP）
--web-port / -wp	❌ 可选	9090	8080	Web UI 端口

✅ 脚本功能
检测项	是否实现	说明
列出 Topics（未授权访问）	✅	检测基础访问权限
消费消息（读取数据）	✅	检测数据泄露风险
生产消息（写入数据）	✅	检测数据篡改风险
访问 Web 管理界面	✅	检测无认证 UI 漏洞

▶️ 只传 IP，其他全默认：
python3 exp_kafka.py --kafka-ip 192.168.193.48

▶️ 自定义所有参数：
python3 exp_kafka.py --kafka-ip 192.168.193.48 --kafka-port 9093 --web-ip 192.168.193.49 --web-port 8080
