import socket
import json
import datetime
import time
from PBFT import *
from nacl.signing import VerifyKey
import ecc

# 这里的client和node之间有什么关系嘛?实际上client是node的服务器,node是客户端
file = "ports.json"
# 读取ports.json文件,返回一个stream
with open(file) as ports_format:
    # 加载stream到ports
    ports = json.load(ports_format)
clients_starting_port = ports["clients_starting_port"]
clients_max_number = ports["clients_max_number"]

# 按照顺序初始化node和client的端口号,放到nodes_ports和clients_ports两个列表当中
nodes_starting_port = ports["nodes_starting_port"]
nodes_max_number = ports["nodes_max_number"]

nodes_ports = [(nodes_starting_port + i) for i in range(0, nodes_max_number)]
clients_ports = [(clients_starting_port + i) for i in range(0, clients_max_number)]

# request的标准格式文档
global request_format_file
request_format_file = "messages_formats/request_format.json"


class Client:
    """类Client:在__init___方法中为client绑定了ip地址和端口号,并且监听其他节点的信息"""

    def __init__(self, client_id, waiting_time_before_resending_request):
        """初始化client"""

        self.client_id = client_id
        self.client_port = clients_ports[client_id]
        # 初始化,设置默认的主节点id值为0
        self.primaryNode_id = 0

        # 使client充当server,接收reply,s为socket对象
        # 第一个参数表明AF_INET表明socket网络层使用IP协议,第二个参数SOCK)STREAM表明传输层使用TCP协议
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # 设置socket的超时时间
        s.settimeout(waiting_time_before_resending_request)

        # 返回当前host的域名
        host = socket.gethostname()
        # 将套接字绑定到地址, 在AF_INET下, 以元组（host, port） 的形式表示地址.SOCKET绑定地址和端口。
        s.bind((host, self.client_port))
        # 使socket处于监听状态,参数表示最多监听num个客户端.backlog指定在拒绝连接之前,操作系统可以挂起的最大连接数量.该值至少为1,大部分应用程序设为5就可以了。
        s.listen()
        # 存储该socket放到client类里
        self.socket = s
        # 用于发送requests但是没有收到回复,这里起到一个日志的作用,用于保存发送了request但是没有收到任何reply.论文中可见，这里应该改名为log更为合适
        self.sent_requests_without_answer = []  # Requests the client sent but didn't get an answer yet
        # 记录发送某个request的时间,字典{request:sending_time}
        self.sending_time = {}
    def broadcast_request(self, request_message, nodes_ids_list, sending_time,
                          f):  # This function is executed if the primary node doesn't receive the request. It is then broadcasted to all the nodes
        """如果client没有收到request,那么client向全部节点广播request，可能触发视图切换协议"""

        # 依次读取所有节点的端口号(除了主节点以外),再依次发送request给各节点
        for node_id in nodes_ids_list:
            if node_id != self.primaryNode_id:
                node_port = nodes_ports[node_id]
                sending_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                host = socket.gethostname()
                # 连接到address处的套接字.一般address的格式为元组（hostname, port）,如果连接出错,返回socket.error错误.
                sending_socket.connect((host, node_port))
                sending_socket.send(request_message)

        # 后续代码,起一个日志的作用
        # Waiting for answers等待应答
        answered_nodes = []  # list of nodes ids that answered the request列表,存储回复该request的节点
        similar_replies = 0  # Initiate the number of similar replies to 0 then takes the max ...初始化相同回答的回复,然后取最大值
        # 存储回复的字典,键是[时间戳, 结果]，值是该时间内收到reply的次数(翻译存疑)
        replies = {}  # A dictionary of replies, keys have the form:[timestamp,result] and the values are the number of time this reply was received
        s = self.socket
        # 设置该client的超时时间为5second
        s.settimeout(5.0)
        while True:
            try:
                # 连接上各节点,接收信息
                c, _ = s.accept()
            # 除非多次尝试超时,否则再次广播request给各节点
            except socket.timeout:
                if len(self.sent_requests_without_answer) != 0:
                    print("No received reply," + "client{client}已广播request给有的节点".format(client=self.client_id))
                    self.broadcast_request(request_message, nodes_ids_list, sending_time, f)
                break
            # 接收message
            received_message = c.recv(2048)
            # 从收到的data将公钥和message分离开
            [received_message, public_key] = received_message.split(b'split')

            # 后面的内容与send_to_primary()是一模一样的,也就是共识环节
            # Create a VerifyKey object from a hex serialized public key
            verify_key = VerifyKey(public_key)
            received_message = verify_key.verify(received_message).decode()
            received_message = received_message.replace("\'", "\"")
            received_message = json.loads(received_message)
            print("Client %d received message: %s" % (self.client_id, received_message))
            answering_node_id = received_message["node_id"]
            if (answering_node_id not in answered_nodes):
                answered_nodes.append(answering_node_id)
                request_timestamp = received_message["timestamp"]
                result = received_message["result"]
                response = [request_timestamp, result]
                str_response = str(response)
                # Increment the number of received replies:
                if str_response not in replies:
                    replies[str_response] = 1
                else:
                    replies[str_response] = replies[str_response] + 1
                if (replies[str_response] > similar_replies):
                    similar_replies += 1
                    if similar_replies == (f + 1):
                        receiving_time = time.time()
                        duration = receiving_time - sending_time
                        number_of_messages = reply_received(received_message["request"], received_message["result"])
                        print("Client %d got reply within %f seconds. The network exchanged %d messages" % (self.client_id, duration, number_of_messages))
                        self.sent_requests_without_answer.remove(received_message["request"])

    def send_request(self, request, nodes_ids_list,
                        f):  # Sends a request to the primary and waits for f+1 similar answers
        """client给主节点发送request然后等该至少f+1个相同的reply;启动一个计时器,当计时器到期没有收到reply时,对外广播request,如果节点发现pre-prepare日志中没有收到对应的request,
        则触发视图切换协议,对外广播view-change信息,并且不接受除checkpoint、view-change、new-view信息"""
        # 标志位,用来记录实验数据中,client收到的f+1时,第一时间
        flag = 0
        # 打开一个文件为啥要循环两次???
        with open(request_format_file):
            with open(request_format_file) as request_format:
                request_message = json.load(request_format)

        # 按照request_format,准备写入request的信息:message={request,operation,timestamp,client},此处和论文相比少了一个operation:比如transaction,client_id能作为序列号吗？
        now = datetime.datetime.now().timestamp()
        request_message["timestamp"] = now
        request_message["request"] = request
        request_message["client_id"] = self.client_id
        digest = hashlib.sha256(request.encode()).hexdigest()
        # 将发送的request加上签名
        request_message = ecc.generate_sign(request_message)

        # 客户端将request的信息广播给所有的节点
        for node_id in nodes_ids_list:
            node_port = nodes_ports[node_id]
            sending_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            host = socket.gethostname()
            # 连接到address处的套接字.一般address的格式为元组（hostname, port）,如果连接出错,返回socket.error错误.
            sending_socket.connect((host, node_port))
            print("client{client_id}已经给{node_id}发送了{request}".format(client_id=self.client_id, node_id=node_id, request=str(request_message)))
            sending_socket.send(request_message)

        # 如果该次发送的request,已发送的且没收到reply,添加当前request发送时间----------->添加进日志,那么已经收到了f+1相同的答复是不是该删除日志
        if (request not in self.sent_requests_without_answer):
            self.sent_requests_without_answer.append(request)
            self.sending_time[digest] = time.time()

        # 客户端request发送给网络的时间,用来计时,多久没有收到reply后重传
        sending_time = time.time()  # This is the time when the client's request was sent to the network客户端发送给网络的时间
        answered_nodes = []  # list of nodes ids that answered the request回复该request的节点列表
        similar_replies = 0  # Initiate the number of similar replies to 0 then takes the max ...初始化相同的回复,收到一条就增加数量
        # 存储回复的字典,键是[时间戳, 结果]，值是该时间内收到reply的次数(翻译存疑)
        replies = {}  # A dictionary of replies, keys have the form:[timestamp,result] and the values are the number of time this reply was received
        # 一个字典，为每个节点存储它为当前请求提供的应答
        nodes_replies = {}  # A dictionary that stores, for each node, the reply it gave for the current request
        s = self.socket
        accepted_reply = ""  # The accepted result for the current

        # 客户端持续监听来自node的reply.该部分会触发视图切换协议
        while True:
            try:
                s = self.socket
                # 客户端接受TCP的连接.返回一个(hostaddr, port),这里取[0]就是为了获取client的域名
                sender_socket = s.accept()[0]

            # socket超时：一种是单纯的超时,没有被发送出去,呢么选择重发;另一种情况,已经发出去了但是收不到主节点的回复,广播重发request,可能触发视图切换
            except socket.timeout:
                # 如果已发送的request都得到了reply,那么就是单纯的网络超时,重新建立连接:accepted_reply
                if len(self.sent_requests_without_answer) == 0:
                    continue
                else:
                    # 否则就是当前的request没有收到reply,打印之
                    print("No received reply" + "触发视图切换!")
                    # Broadcasting request:# 没有收到reply,执行广播函数,向所有节点广播该request
                    self.broadcast_request(request_message, nodes_ids_list, sending_time, f)
                    socket.close()
                    break
            # 正常工作的情形,收到了reply,需要对比签名,检查回复的内容是否一致

            # recv用于接受信息,当固定值为2048bytes时,会一直等到2048位,如此不是大大增加了时延?更多信息请查看:http://t.csdn.cn/VvSId
            received_message = sender_socket.recv(2048)

            # 打印:client_id收到了message
            print("Client %d got message: %s" % (self.client_id, received_message))
            # 已经收到了节点回送给client的reply,因此关闭掉节点发送的socket
            sender_socket.close()
            # 分离开签名和message
            [received_message, public_key] = received_message.split(b'split')
            # Create a VerifyKey object from a hex serialized public key从十六进制序列化的公钥创建一个VerifyKey对象
            verify_key = VerifyKey(public_key)
            # 将二进制message译码
            received_message = verify_key.verify(received_message).decode()
            # 为啥要加上这个?
            received_message = received_message.replace("\'", "\"")
            # 解析为json格式
            received_message = json.loads(received_message)
            # 打印:client已经收到了信息
            print("Client %d received message: %s" % (self.client_id, received_message))

            # 保存信息,result只有一个,reply里有result,暂且看成证书和签名,result具有唯一性和不可篡改性
            answering_node_id = received_message["node_id"]
            request_timestamp = received_message["timestamp"]
            result = received_message["result"]
            response = [request_timestamp, result]

            # 保存信息,达成共识
            if (answering_node_id not in answered_nodes):
                # 将已经回复的节点添加进列表
                answered_nodes.append(answering_node_id)
                str_response = str(response)

                # 保存节点回复的信息
                nodes_replies[answering_node_id] = str_response
                # 记录收到的reply的个数,同一个节点id可能会发送两条以上的reply,因此timestamp十分重要.replies负责收集所有的reply
                if str_response not in replies:
                    replies[str_response] = 1
                else:
                    replies[str_response] = replies[str_response] + 1

                # 计算相同的个数,也就是共识.这里直接用count或者length就好,干嘛要这样呢
                if (replies[str_response] > similar_replies):
                    similar_replies += 1
                if similar_replies >= (f + 1):
                    # 没有return,定义了却不使用的意义在哪里?
                    accepted_reply = result
                    accepted_response = str_response
                    receiving_time = time.time()
                    duration = receiving_time - sending_time
                    print("client{name}已经达成共识".format(name=self.client_id))
                    # while flag == 0 ----> 将共识时间输出到文件道中
                    while flag == 0:
                        print("达成共识")
                        flag = 1
                    #     with open('data/90node.txt', 'a') as file:
                    #         print(str(duration), file=file)
                    #     flag = 1
                    number_of_messages = reply_received(received_message["request"], received_message["result"])
                    if similar_replies == (f + 1):
                        print("Client %d got reply within %f seconds. The network exchanged %d messages" % (
                        self.client_id, duration, number_of_messages))
                    if (received_message["request"] in self.sent_requests_without_answer):
                        self.sent_requests_without_answer.remove(received_message["request"])

    def receive_viewChange(self):
        """对收到的视图切换信息进行校验,确认收到f+1个相同主节点的id值后,修改节点存储的主节点primaryNode_id 值"""
        return self.primaryNode_id
