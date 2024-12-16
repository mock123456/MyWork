import threading
from threading import Lock
import socket
import json
import time
import hashlib
import ecc
from nacl.signing import SigningKey
from nacl.signing import VerifyKey

# 初始化网络,设置客户端和节点的数量、端口号
ports_file = "ports.json"
with open(ports_file) as ports_format:
    ports = json.load(ports_format)
clients_starting_port = ports["clients_starting_port"]
clients_max_number = ports["clients_max_number"]

nodes_starting_port = ports["nodes_starting_port"]
nodes_max_number = ports["nodes_max_number"]

nodes_ports = [(nodes_starting_port + i) for i in range(0, nodes_max_number)]
clients_ports = [(clients_starting_port + i) for i in range(0, clients_max_number)]

# 设置PBFT中每个消息的标准格式，为啥要用英文呢,都写错了
preprepare_format_file = "messages_formats/preprepare_format.json"
prepare_format_file = "messages_formats/prepare_format.json"
commit_format_file = "messages_formats/commit_format.json"
reply_format_file = "messages_formats/reply_format.json"
checkpoint_format_file = "messages_formats/checkpoint_format.json"
checkpoint_vote_format_file = "messages_formats/checkpoint_vote_format.json"
view_change_format_file = "messages_formats/view_change_format.json"
new_view_format_file = "messages_formats/new_view_format.json"


def run_PBFT(nodes, proportion, checkpoint_frequency0, clients_ports0, timer_limit_before_view_change0):
    """运行Pbft共识算法"""

    # 这里的p难道是节点的故障比例?
    global p
    p = proportion

    # 该字典存储每个request从pre_prepare到reply过程中,通信信息的数量,number_of_messages={"request":number_of_exchanged_messages,...}
    global number_of_messages
    number_of_messages = {}  # This dictionary will store for each request the number of exchanged messages from preprepare to reply: number_of_messages={"request":number_of_exchanged_messages,...}

    # 这个字典辨别request是否得到了reply
    global replied_requests
    replied_requests = {}  # This dictionary tells if a request was replied to (1) or not

    # 视图切换的时间限制
    global timer_limit_before_view_change
    timer_limit_before_view_change = timer_limit_before_view_change0

    # client的端口号
    global clients_ports
    clients_ports = clients_ports0

    # 字典,存储client收到的所有reqeust
    global accepted_replies
    accepted_replies = {}  # Dictionary that stores for every request the reply accepted by the client

    # 总节点的数量n
    global n
    n = 0  # total nodes number

    # 最多可容纳的故障节点的个数(n-1)//3,取整
    global f
    f = (n - 1) // 3  # 可容纳的故障节点的最大数量,应该在每次总节点数量变动时改变

    # 列表:存储节点的node_id
    global the_nodes_ids_list
    the_nodes_ids_list = [i for i in range(n)]

    # 下一个节点的id(存疑),每当一个新的节点被初始化时,j += 1
    global j  # next id node (each time a new node is instantiated, it is incremented)
    j = 0

    # 字典:键是client的id,值是这个request的时间戳. {client_id:timestamp}.初始化时为空
    global requests  # a dictionary where keys are the clients' ids and the value is the timestamp of their last request
    requests = {}  # Initiate as an empty dictionary

    # 检查点协议运行的频率
    global checkpoint_frequency
    checkpoint_frequency = checkpoint_frequency0

    # 此处翻译存疑:在每次新的request发起时,重置为0,该参数为检查点协议服务
    global sequence_number
    sequence_number = 1  # Initiate the sequence number to 0 and increment it with each new request - we choosed 0 so that we can have a stable checkpoint at the beginning (necessary for a view change)

    # 节点的字典,关联着node对象,可以通过node0、node1、node2...来调用node的方法和属性
    global nodes_list
    nodes_list = {}

    # 总处理信息,这个是发送一个request时,通过网络发送message的数量
    global total_processed_messages
    total_processed_messages = 0  # The total number of preocessed messages - this is the total number of send messages through the netwirk while processing a request

    # Nodes evaluation metrics:
    # 每个节点处理的信息数
    global processed_messages
    processed_messages = []  # Number of processed messages by each node

    # 翻译存疑:这是网络中所有节点之间处理消息的比率——计算为节点发送的消息与所有节点通过网络发送的消息的比率
    global messages_processing_rate
    messages_processing_rate = []  # This is the rate of processed messages among all the nodes in the network - calculated as the ratio of messages sent by the node to all sent messages through the network by all the nodes

    # 参与共识算法的节点的id
    global consensus_nodes
    consensus_nodes = []

    # python多线程执行,提高运行的速度:执行run_nodes函数
    threading.Thread(target=run_nodes, args=(nodes,)).start()


def run_nodes(nodes):
    """根据不同的node类型,初始化不同的节点,启动不同的网络,该方法存在不足,无法以变量名控制对象node,解决办法是初始化时引入一个字典作为node的名称"""
    # j实际为node_id,j=1,j=2,j=3...
    global j
    # node总数n
    global n
    # 最大故障节点数量f,其中n = 2f + 1   ---->   f = (n-1) // 3
    global f

    # total_initial_nodes = sum(node_type[1] for node_type in nodes[0])

    # Starting nodes:初始化节点      这里是初始化节点的第一种方法
    last_waiting_time = 0
    # 此处waiting_time实际上就是0,该变量没有取到合适的名称,j实际为node_id,j=1,j=2,j=3...
    # for waiting_time in nodes:
    #     for tuple in nodes[waiting_time]:
    #         # 源代码是for _ in range(tuple[1]):tuple[1]是float,不能解释为int-->不能给range()用
    #         for _ in range(int(tuple[1])):
    #             time.sleep(waiting_time - last_waiting_time)
    #             last_waiting_time = waiting_time
    #             node_type = tuple[0]
    #             if (node_type == "honest_node"):
    #                 node = HonestNode(node_id=j)
    #             elif (node_type == "non_responding_node"):
    #                 node = NonRespondingNode(node_id=j)
    #             elif (node_type == "faulty_primary"):
    #                 node = FaultyPrimary(node_id=j)
    #             elif (node_type == "slow_nodes"):
    #                 node = SlowNode(node_id=j)
    #             elif (node_type == "faulty_node"):
    #                 node = FaultyNode(node_id=j)
    #             elif (node_type == "faulty_replies_node"):
    #                 node = FaultyRepliesNode(node_id=j)
    #             # 打开Socket,让node持续监听消息
    #             threading.Thread(target=node.receive, args=()).start()
    #             nodes_list.append(node)
    #             the_nodes_ids_list.append(j)
    #             processed_messages.append(0)
    #             messages_processing_rate.append(0)  # Initiated with 0
    #             consensus_nodes.append(j)
    #             n = n + 1
    #             f = (n - 1) // 3
    #             print("%s node %d started" %(node_type,j))
    #             j = j + 1

    # 打印共识的节点(可选)
    # print(consensus_nodes)

    # 初始化节点,根据几点类型初始化不同的节点,将node对象以字典的形式保存在node_list列表里,通过列表调用node对象的方法和属性
    # 此处waiting_time实际上就是0,该变量没有取到合适的名称,j实际为node_id,j=1,j=2,j=3...
    for key in nodes:
        while nodes[key] != 0:
            if key == "faulty_primary":
                nodes_list["node" + str(j)] = FaultyPrimary(node_id=j)
            elif key == "honest_node":
                nodes_list["node" + str(j)] = HonestNode(node_id=j)
            elif key == "non_responding_node":
                nodes_list["node" + str(j)] = NonRespondingNode(node_id=j)
            elif key == "slow_nodes":
                nodes_list["node" + str(j)] = SlowNode(node_id=j)
            elif key == "faulty_node":
                nodes_list["node" + str(j)] = FaultyNode(node_id=j)
            elif key == "faulty_replies_node":
                nodes_list["node" + str(j)] = FaultyRepliesNode(node_id=j)
            nodes[key] -= 1
            # 打开Socket,让node持续监听消息
            threading.Thread(target=nodes_list["node" + str(j)].receive, args=()).start()
            the_nodes_ids_list.append(j)
            processed_messages.append(0)
            messages_processing_rate.append(0)  # Initiated with 0
            consensus_nodes.append(j)
            n = n + 1
            f = (n - 1) // 3
            # print("%s node %d started" % (key, j))
            j = j + 1


# request被网络处理的次数
global processed_requests  # This is the total number of requests processed by the network
processed_requests = 0
# request被首次回复的时间
global first_reply_time


def reply_received(request,
                   reply):  # This method tells the nodes that the client received its reply so that they can know the accepted reply
    """这个方法告诉节点,client已经收到了reply"""
    global processed_requests
    processed_requests = processed_requests + 1

    if processed_requests == 1:
        global first_reply_time
        first_reply_time = time.time()

    last_reply_time = time.time()

    if processed_requests % 5 == 0:  # We want to stop counting at 100 for example 例如当数量为100时,我们想要停止计数
        print("Network validated %d requests within %f seconds" % (
            processed_requests, last_reply_time - first_reply_time))
    replied_requests[request] = 1
    accepted_replies[request] = reply

    return number_of_messages[request]


# 这个函数应该没有存在的必要性
# def get_primary_id():
#     """初始化时默认id 0 为主节点,当视图切换协议成功后,返回新的主节点id值，这个"""
#     # 暂时修改一下代码
#     # node_0 = nodes_list[0]
#     # return node_0.primary_node_id
#     primary_id = 0
#     # 当视图接环协议成功后,client接收到新的主节点id值
#     return primary_id


def get_nodes_ids_list():
    """返回node的列表"""
    return consensus_nodes


def get_f():
    """这个f的作用是什么呢？？？"""
    return f


class Node():
    """每个副本应保留副本的状态:包括接收的message日志,当前副本的视图证书"""

    def __init__(self, node_id):
        # 副本编号i
        self.node_id = node_id
        self.node_port = nodes_ports[node_id]
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        host = socket.gethostname()
        s.bind((host, self.node_port))
        s.listen()
        self.socket = s
        self.view_number = 0  # Initiated with 1 and increases with each view change初始值为1,在每次视图切换的时候增加
        # 每个节点存储着当前的主节点id,默认为0
        self.primary_node_id = 0
        self.preprepares = {}  # Dictionary of tuples of accepted preprepare messages: preprepares=[(view_number,sequence_number):digest]字典,存储元组,元组存储接收到的pre-preprepares
        self.prepared_messages = []  # set of prepared messages 集合,存储prepared消息
        self.replies = {}  # Maintain a dictionary of the last reply for each client: replies={client_id_1:[last_request_1,last_reply_1],...}   字典,保存最近的来自与每个client的reply
        self.message_reply = []  # List of all the reply messages 列表：所有reply的信息
        self.prepares = {}  # Dictionary of accepted prepare messages: prepares = {(view_number,sequence_number,digest):[different_nodes_that_replied]}
        self.commits = {}  # Dictionary of accepted commit messages: commits = {(view_number,sequence_number,digest):[different_nodes_that_replied]}
        self.message_log = []  # Set of accepted messages
        self.last_reply_timestamp = {}  # A dictionary that for each client, stores the timestamp of the last reply字典:每个副本会记住最后一条发送给客户端的message.时间戳?
        self.checkpoints = {}  # Dictionary of checkpoints: {checkpoint:[list_of_nodes_that_voted]} 检查点字典：{检查点:[参加选举主节点的节点列表]}
        self.checkpoints_sequence_number = []  # List of sequence numbers where a checkpoint was proposed 提出检查点的序列号列表
        self.stable_checkpoint = {"message_type": "CHECKPOINT", "sequence_number": 0,
                                  "checkpoint_digest": "the_checkpoint_digest",
                                  "node_id": self.node_id}  # The last stable checkpoint    最近的稳定的检查点
        self.stable_checkpoint_validators = []  # list of nodes that voted for the last stable checkpoint 为最后一个稳定检查点投票的节点列表
        self.h = 0  # The low water mark = sequence number of the last stable checkpoint 最低水位线------最近一个稳定检查点的序列号
        self.H = self.h + 200  # The high watermark, proposed value in the original article   高水位线，建议值在原文中
        # 字典,存储节点发送给主节点的request和时间戳,如果收到对应的pre-prepare或者启动了视图切换,就清空
        self.request = {}
        # 字典:接收pre-prepare的时间,附带一个他们被接收的时间,因此,计时器到期时,node可以发起视图切换。{"request":starting_time...},在执行一次后,这个request会被抛弃
        self.accepted_requests_time = {}  # This is a dictionary of the accepted preprepare messages with the time they were accepted so that one the timer is reached, the node starts a view change. The dictionary has the form : {"request":starting_time...}. the request is discarded once it is executed.
        # 字典:接收并回复pre-prepare信息得时间.这个字典的格式: {"request": ["reply",replying_time]...}. ，执行一次后就会被抛弃.
        self.replies_time = {}  # This is a dictionary of the accepted preprepare messages with the time they were replied to. The dictionary has the form : {"request": ["reply",replying_time]...}. the request is discarded once it is executed.
        # 收到视图切换信息的字典(可以包含节点自己发送的视图切换信息),如果这个节点是新视图中的主节点，格式如下{new_view_number:[list_of_view_change_messages]}
        self.received_view_changes = {}  # Dictionary of received view-change messages (+ the view change the node itself sent) if the node is the primary node in the new view, it has the form: {new_view_number:[list_of_view_change_messages]}
        # 节点询问的视图序号？翻译存疑
        self.asked_view_change = []  # view numbers the node asked for

    def process_received_message(self, received_message, waiting_time):
        """根据node收到不同的消息类型,作出不同的处理"""
        global total_processed_messages
        message_type = received_message["message_type"]
        if (message_type == "REQUEST"):
            self._requestmessage(received_message)
        if message_type == "PREPREPARE":
            self._preprepare_request(received_message)
        elif message_type == "PREPARE":
            self._prepare_request(received_message)
        elif message_type == "COMMIT":
            total_processed_messages += 1
            node_id = received_message["node_id"]
            processed_messages[node_id] += 1
            messages_processing_rate[node_id] = processed_messages[node_id] / total_processed_messages

            request = received_message["request"]
            digest = hashlib.sha256(request.encode()).hexdigest()
            requests_digest = received_message["request_digest"]

            number_of_messages[received_message["request"]] = number_of_messages[received_message["request"]] + 1
            timestamp = received_message["timestamp"]
            client_id = received_message["client_id"]

            # TO DO: Make sure that h<sequence number<H 确保 h< sequence number <H
            client_id = received_message["client_id"]
            timestamp = received_message["timestamp"]
            if (self.view_number == received_message["view_number"]):
                sequence_number = received_message["sequence_number"]

                self.message_log.append(received_message)
                tuple = (received_message["view_number"], received_message["sequence_number"],
                         received_message["request_digest"])
                self.commits[tuple] = self.commits[tuple] + 1 if tuple in self.commits else 1
                i = 0

                if (self.commits[tuple] == (2 * f + 1) and (tuple in self.prepares)):
                    if (client_id in self.last_reply_timestamp and
                            (self.last_reply_timestamp[client_id] <
                             timestamp) or client_id not in
                            self.last_reply_timestamp):
                        i = 1

                    if i == 1:
                        time.sleep(waiting_time)
                        # print(self.node_id,"sleep",waiting_time)
                        reply = self.send_reply_message_to_client(received_message)
                        if request in self.accepted_requests_time:
                            request_accepting_time = self.accepted_requests_time[request]
                            self.replies_time[request] = [reply, time.time() - request_accepting_time]
                            number_of_messages[received_message["request"]] = number_of_messages[
                                                                                  received_message["request"]] + 1
                            self.accepted_requests_time[received_message["request"]] = -1
                        client_id = received_message["client_id"]
                        self.replies[client_id] = [received_message, reply]
                        self.last_reply_timestamp[client_id] = timestamp

                        if (
                                sequence_number % checkpoint_frequency == 0 and sequence_number not in self.checkpoints_sequence_number):  # Creating a new checkpoint at each checkpoint creation period
                            self._process_check_point(
                                sequence_number,
                                received_message,
                                reply,
                            )
        elif message_type == "CHECKPOINT":
            lock = Lock()
            lock.acquire()
            for message in self.message_reply:
                if (message["message_type"] == "REPLY" and message["sequence_number"] == received_message[
                    "sequence_number"]):
                    reply_list = [message["request_digest"], message["client_id"], message["result"]]
                    reply_digest = hashlib.sha256(str(reply_list).encode()).hexdigest()
                    if (reply_digest == received_message["checkpoint_digest"]):
                        with open(checkpoint_vote_format_file):
                            with open(checkpoint_vote_format_file) as checkpoint_vote_format:
                                checkpoint_vote_message = json.load(checkpoint_vote_format)
                        checkpoint_vote_message["sequence_number"] = received_message["sequence_number"]
                        checkpoint_vote_message["checkpoint_digest"] = received_message["checkpoint_digest"]
                        checkpoint_vote_message["node_id"] = self.node_id

                        # Generate a new random signing key生成随机的私钥签名
                        signing_key = SigningKey.generate()
                        signed_checkpoint_vote = signing_key.sign(str(checkpoint_vote_message).encode())
                        verify_key = signing_key.verify_key
                        public_key = verify_key.encode()

                        checkpoint_vote_message = signed_checkpoint_vote + (b'split') + public_key

                        # 引入VRF

                        self.send(received_message["node_id"], checkpoint_vote_message)

            lock.release()

        elif message_type == "VOTE":
            lock = Lock()
            lock.acquire()
            for checkpoint in self.checkpoints:
                checkpoint = checkpoint.replace("\'", "\"")
                checkpoint = json.loads(checkpoint)
                if (received_message["sequence_number"] == checkpoint["sequence_number"] and received_message[
                    "checkpoint_digest"] == checkpoint["checkpoint_digest"]):
                    node_id = received_message["node_id"]
                    if (node_id not in self.checkpoints[str(checkpoint)]):
                        self.checkpoints[str(checkpoint)].append(node_id)
                        if (len(self.checkpoints[str(checkpoint)]) <= (2 * f + 1)):
                            # This will be the last stable checkpoint
                            self.stable_checkpoint = checkpoint
                            self.stable_checkpoint_validators = self.checkpoints[str(checkpoint)]
                            self.h = checkpoint["sequence_number"]
                            # TO DO: Delete checkpoints and messages log <= n
                            self.checkpoints.pop(str(checkpoint))
                            for message in self.message_log:
                                if (
                                        message["message_type"]
                                        != "REQUEST"
                                ) and (message[
                                           "sequence_number"]
                                       <= checkpoint[
                                           "sequence_number"]
                                ):
                                    self.message_log.remove(message)
                            break
            lock.release()

        elif message_type == "VIEW-CHANGE":
            new_asked_view = received_message["new_view"]
            # 如果新选的主节点就是当前的节点
            if (new_asked_view % len(
                    consensus_nodes) == self.node_id):
                node_requester = received_message["node_id"]
                # 保存日志中没有的内容:收到的某个节点的视图切换信息
                if new_asked_view not in self.received_view_changes:
                    self.received_view_changes[new_asked_view] = [received_message]
                else:
                    requested_nodes = [
                        request["node_id"] for request in
                        self.received_view_changes[new_asked_view]
                    ]
                    if node_requester not in requested_nodes:
                        self.received_view_changes[new_asked_view].append(received_message)
                #  如果收到视图切换的信息有2f个
                if len(self.received_view_changes[new_asked_view]) == 2 * f:
                    # The primary sends a view-change message for this view if it didn't do it before主节点发送view-change消息,如果他之前没有发送过这个协议
                    if new_asked_view not in self.asked_view_change:
                        view_change_message = self.broadcast_view_change()
                        self.received_view_changes[new_asked_view].append(view_change_message)
                    # Broadcast a new view message:广播视图切换消息
                    with open(new_view_format_file) as new_view_format:
                        new_view_message = json.load(new_view_format)
                    new_view_message["new_view_number"] = new_asked_view
                    V = self.received_view_changes[new_asked_view]
                    new_view_message["V"] = V
                    # Creating the "O" set of the new view message:
                    # Initializing min_s and max_s:
                    min_s = 0
                    max_s = 0
                    if (len(V) > 0):
                        sequence_numbers_in_V = [view_change_message["last_sequence_number"] for view_change_message in
                                                 V]
                        min_s = min(sequence_numbers_in_V)  # min sequence number of the latest stable checkpoint in V V中最近稳定检查点的最小序列号
                    sequence_numbers_in_prepare_messages = [message["sequence_number"] for message in self.message_log
                                                            if message["message_type"] == "PREPARE"]
                    if len(sequence_numbers_in_prepare_messages) != 0:
                        max_s = max(sequence_numbers_in_prepare_messages)

                    O = []

                    if (max_s >= min_s):
                        # 为new_asked_view创建一个pre-prepare-view消息,消息的序列号介于max_s和min_s之间
                        # Creating a preprepare-view message for new_asked_view for each sequence number between max_s and min_s
                        for s in range(min_s, max_s):
                            with open(preprepare_format_file) as preprepare_format:
                                preprepare_message = json.load(preprepare_format)
                            preprepare_message["view_number"] = new_asked_view
                            preprepare_message["sequence_number"] = s
                            i = 0  # There is no set Pm in P with sequence number s - In our code: there is no prepared message in P where sequence number = s (case 2 in the paper) => It turns i=1 if we find such a set
                            # P存储之前稳定检查点的prepare消息,如果我记得不错的话
                            P = received_message["P"]
                            v = 0  # Initiate the view number so that we can find the highest one in P
                            for message in P:
                                if (message["sequence_number"]) == s:
                                    i = 1
                                    if (message["view_number"] > v):
                                        v = message["view_number"]
                                        d = message["request_digest"]
                                        r = message["request"]
                                        t = message["timestamp"]
                                        c = message["client_id"]

                                        preprepare_message["request"] = r
                                        preprepare_message["timestamp"] = t
                                        preprepare_message["client_id"] = c

                                # Restart timers:
                                for request in self.accepted_requests_time:
                                    self.accepted_requests_time[request] = time.time()

                                preprepare_message["request_digest"] = "null" if (i == 0) else d
                                O.append(preprepare_message)
                                self.message_log.append(preprepare_message)

                    new_view_message["O"] = O

                    if (min_s >= self.stable_checkpoint["sequence_number"]):
                        # The primary node enters the new view
                        self.view_number = new_asked_view

                        # Change primary node (locally first then broadcast view change)
                        self.primary_node_id = self.node_id

                        print("当前节点是node{id0},主节点是{id1},并且广播了new-view消息".format(id0=self.node_id,id1=self.primary_node_id))
                        list1 = []
                        for node in consensus_nodes:
                            if node != self.node_id:
                                list1.append(node)
                        new_view_message = ecc.generate_sign(new_view_message)
                        self.broadcast_message(list1, new_view_message)


        elif (message_type == "NEW-VIEW"):
            """更改当前节点的主节点,并且继续之前已经pre-prepare和prepare的内容"""
            # TO DO : Verify the set O in the new view message

            # Restart timers:重启request的计时器
            for request in self.accepted_requests_time:
                self.accepted_requests_time[request] = time.time()

            O = received_message["O"]
            # Broadcast a prepare message for each preprepare message in O
            if len(O) != 0:
                for message in O:
                    if (received_message["request_digest"] != "null"):
                        self.message_log.append(message)
                        prepare_message = self.broadcast_prepare_message(message, consensus_nodes)
                        self.message_log.append(prepare_message)
            self.view_number = received_message["new_view_number"]
            self.primary_node_id = received_message["new_view_number"] % n
            print("node{id0}当前的主节点是{id1}".format(id0=self.node_id,id1=self.primary_node_id))
            self.asked_view_change.clear()

    def _process_check_point(self, sequence_number, received_message, reply):
        with open(checkpoint_format_file) as checkpoint_format:
            checkpoint_message = json.load(checkpoint_format)
        checkpoint_message["sequence_number"] = sequence_number
        checkpoint_message["node_id"] = self.node_id
        checkpoint_content = [received_message["request_digest"], received_message["client_id"],
                              reply]  # We define the current state as the last executed request
        checkpoint_message["checkpoint_digest"] = hashlib.sha256(str(checkpoint_content).encode()).hexdigest()
        self.checkpoints_sequence_number.append(sequence_number)
        self.checkpoints[str(checkpoint_message)] = [self.node_id]
        checkpoint_message = ecc.generate_sign(checkpoint_message)
        self.broadcast_message(consensus_nodes, checkpoint_message)

    def _prepare_request(self, received_message):
        """处理收到的prepare消息,正常情况应该对外广播commit消息"""
        # print("{id}正在处理prepare消息".format(id=self.node_id))
        global total_processed_messages
        total_processed_messages += 1
        node_id = received_message["node_id"]
        processed_messages[node_id] += 1
        messages_processing_rate[node_id] = processed_messages[node_id] / total_processed_messages
        request = received_message["request"]
        digest = hashlib.sha256(request.encode()).hexdigest()
        requests_digest = received_message["request_digest"]
        number_of_messages[received_message["request"]] = number_of_messages[received_message["request"]] + 1
        timestamp = received_message["timestamp"]
        client_id = received_message["client_id"]
        the_sequence_number = received_message["sequence_number"]
        the_request_digest = received_message["request_digest"]
        tuple = (
            received_message["view_number"], received_message["sequence_number"], received_message["request_digest"])
        node_id = received_message["node_id"]
        if ((received_message["view_number"] == self.view_number)):
            self.message_log.append(received_message)
            if (tuple not in self.prepares):
                self.prepares[tuple] = [node_id]
            elif (node_id not in self.prepares[tuple]):
                self.prepares[tuple].append(node_id)
        # Making sure the node inserted in its message log: a pre-prepare for m in view v with sequence number n
        # 第二种情形:确保节点插入了日志：2f个来自不同点的背书,而且能够和之前的pre-prepare对应上(相同的视图编号,序列号和哈希值),p用来作flag
        p = 0
        for message in self.message_log:
            if ((message["message_type"] == "PREPREPARE") and (
                    message["view_number"] == received_message["view_number"]) and (
                    message["sequence_number"] == received_message["sequence_number"]) and (
                    message["request"] == received_message["request"])):
                p = 1
                break
        # Second condition: Making sure the node inserted in its message log: 2f prepares from different backups that match the pre-preapare (same view, same sequence number and same digest)
        # print(len(self.prepares[tuple]))
        # 确保收到了2f个相同的prepare,包括自己
        # print("f={num}".format(num=f))
        # print("2*f={num}".format(num=2*f))
        # print("{node}已经收到了{n}个相同的prepare,总共要收集{n1}个prepare".format(node=self.node_id,n=self.prepares[tuple], n1=2*f))
        if (p == 1 and len(self.prepares[tuple]) == (
                2 * f)):  # The 2*f received messages also include the node's own received message
            self.prepared_messages.append(received_message)
            self.broadcast_commit_message(prepare_message=received_message, nodes_ids_list=consensus_nodes,
                                          sequence_number=the_sequence_number)
            print("节点{id}已经收到了2*f个相同的prepare,已经广播了commit消息".format(id=self.node_id))

    def _preprepare_request(self, received_message):
        """首先要检查之前的计时器,如果存在计时器,到期前没有收到对应request的pre-prepare,那么发送view-change,
        如果满足上面的条件,那么该节点只接受检查点、视图切换、new-view消息;
        副本节点收到pre-prepare消息,校验视图编号和request后,存入日志,并且广播prepare消息"""

        global total_processed_messages
        # print("{id}正在处理pre-prepare".format(id=self.node_id))
        node_id = received_message["node_id"]
        request = received_message["request"]
        digest = hashlib.sha256(request.encode()).hexdigest()
        requests_digest = received_message["request_digest"]

        # # 首先检查计时器,查看字典是否非空,如果存在,检查计时器是否到期
        # if self.accepted_requests_time:
        #     with open(request_format_file):
        #         with open(request_format_file) as request_format:
        #             request_message = json.load(request_format)
        #             request_message[]

        total_processed_messages += 1
        processed_messages[node_id] += 1
        messages_processing_rate[node_id] = processed_messages[node_id] / total_processed_messages

        number_of_messages[received_message["request"]] = number_of_messages[received_message["request"]] + 1
        timestamp = received_message["timestamp"]
        client_id = received_message["client_id"]
        request = received_message["request"]
        digest = hashlib.sha256(request.encode()).hexdigest()
        requests_digest = received_message["request_digest"]
        view = received_message["view_number"]
        tuple = (view, received_message["sequence_number"])
        # Making sure the digest's request is good + the view number in the message is similar to the view number of the node + We did not broadcast a message with the same view number and sequence number
        if ((digest == requests_digest) and (view == self.view_number)):
            if request not in self.accepted_requests_time:
                self.accepted_requests_time[request] = time.time()  # Start timer
            if tuple not in self.preprepares:
                self.message_log.append(received_message)
                self.preprepares[tuple] = digest
                # 排除掉主节点,只发送给普通节点
                list1 = []
                for node_id in consensus_nodes:
                    if node_id != self.primary_node_id:
                        list1.append(node_id)
                self.broadcast_prepare_message(preprepare_message=received_message, nodes_ids_list=list1)

    def _requestmessage(self, received_message):
        """如果当前节点是主节点,那么应该对外广播pre-prepare(除自己外的节点);
        如果是副本节点，那么启动计时器,计时器到期前没有收到对应request的pre-prepare,那么对外广播new-view消息;
        如果再次收到相同的request,并且计时器到期,广播view-change消息"""
        if (received_message["request"] not in number_of_messages):
            number_of_messages[received_message["request"]] = 0
        if (received_message["request"] not in self.accepted_requests_time):
            self.accepted_requests_time[(received_message["request"])] = time.time()
        if (received_message["request"] not in replied_requests):
            replied_requests[received_message["request"]] = 0
        timestamp = received_message["timestamp"]
        client_id = received_message["client_id"]
        # 如果当前节点是主节点,并且查看日志,之前没有回复过,时间戳也正确---->对外广播pre-prepare
        if (client_id not in self.last_reply_timestamp or timestamp > self.last_reply_timestamp[client_id]):
            if (self.node_id == self.primary_node_id):
                client_id = received_message["client_id"]
                actual_timestamp = received_message["timestamp"]
                last_timestamp = requests[client_id] if (client_id in requests) else 0
                if ((last_timestamp < actual_timestamp) or (
                        last_timestamp == actual_timestamp and (received_message["request"] != reply["request"] for
                                                                reply in self.message_reply))):
                    requests[client_id] = actual_timestamp
                    list = []
                    print()
                    for node in consensus_nodes:
                        if node != self.primary_node_id:
                            list.append(node)
                    self.message_log.append(received_message)
                    self.broadcast_preprepare_message(request_message=received_message, nodes_ids_list=list)
                    print("主节点{id}已经广播了pre-prepare消息".format(id=self.node_id))
            # 当前节点是普通节点,将消息重传给主节点,并且启动一个是计时器,计时器到期之前这个request没有被执行完(没有收到pre-prepare),则触发视图切换协议,对外广播view-change
            else:
                print("{name}的计时器启动,计时器到期没有达成共识,则广播view-change消息".format(name=self.node_id))
                self.send(destination_node_id=self.primary_node_id, message=received_message)

    def receive(self, waiting_time):
        """接收消息,waiting_time是节点收到消息的处理时延
        The waiting_time parameter is for nodes we want to be slow,
        they will wait for a few seconds before processing a message =0 by default"""
        while True:
            s = self.socket
            c, _ = s.accept()
            received_message = c.recv(2048)
            # print(received_message)
            # 打印node已经收到了message
            # print("Node %d got message: %s" % (self.node_id , received_message))
            # 当签名被篡改时,由于无法从消息中解析出public_key,因此会报错,期待两个值,实际上只有一个值
            # socket.close()会收到一个b'',必须排除掉,否则会报错
            if received_message == b'':
                print("socket关闭收到一个空包")
            else:
                a = received_message.split(b'split')
                print("node{id}收到消息{message}\n".format(id=self.node_id, message=a))

                [received_message, public_key] = received_message.split(b'split')

                # Create a VerifyKey object from a hex serialized public key用公钥验证签名的正确性
                verify_key = VerifyKey(public_key)
                received_message = verify_key.verify(received_message).decode()
                received_message = received_message.replace("\'", "\"")
                received_message = json.loads(received_message)
                # 对收到的消息做一个check,判断是否超时等等
                threading.Thread(target=self.check, args=(received_message, waiting_time,)).start()

    def check(self, received_message, waiting_time):
        """
        检验收到的消息
        首先查看是否存在计时器
        """
        global new_view
        i = 0  # Means no timer reached the limit , i = 1 means one of the timers reached their limit当i=0时,计时器正常,当i=1时,有一个计时器超时
        # 如果有一个request计时器被启动,并且self.asked_view_change == 0.说明该副本节点之前重传过request给主节点,并且还没有收到视图切换的消息
        if len(self.accepted_requests_time) != 0 and len(
                self.asked_view_change) == 0:  # Check if the dictionary is not empty检查字典是否为空
            # 检查收到的request是否超时,超时则令self.view_number加一,准备启动视图切换协议
            for request in self.accepted_requests_time:
                if self.accepted_requests_time[request] != -1:
                    actual_time = time.time()
                    timer = self.accepted_requests_time[request]
                    print("{name}计时器过去了{time}\n".format(name=self.node_id, time=actual_time - timer))
                    # 如果重传给主节点的request的计时器超时
                    if (actual_time - timer) >= timer_limit_before_view_change:
                        i = 1  # One of the timers reached their limit一个计时器超时
                        # 此处存在问题,如果new_view超出了最大视图编号呢？正确的做法应该时取模
                        new_view = self.view_number + 1
                        break
        # 存在一个计时器,广播视图切换消息
        if i == 1 and new_view not in self.asked_view_change:
            # Broadcast a view change:一个计时器超时,广播一次视图切换
            print("节点{name}已经广播了视图切换消息".format(name=self.node_id))
            threading.Thread(target=self.broadcast_view_change, args=()).start()
            self.asked_view_change.append(new_view)
            for request in self.accepted_requests_time:
                if self.accepted_requests_time[request] != -1:
                    self.accepted_requests_time[request] = time.time()
        message_type = received_message["message_type"]
        # 接下来只接受检查点、vote、view-change、new-view信息
        if message_type in ["CHECKPOINT", "VOTE", "VIEW-CHANGE", "NEW-VIEW"]:
            threading.Thread(target=self.process_received_message, args=(received_message, waiting_time,)).start()
        # 没有触发视图切换,正常处理request
        elif (i != 1 and len(self.asked_view_change) == 0
              and ((message_type == "REQUEST" or
                    (received_message[
                         "view_number"] == self.view_number)))):  # Only accept messages with view numbers==the view number of the node
            client_id = received_message["client_id"]
            request = received_message["request"]
            if (client_id in self.replies and received_message == self.replies[client_id][0]):
                # print("Node %d: Request already processed" % self.node_id)
                reply = self.replies[client_id][1]
                client_port = clients_ports[client_id]
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect(("localhost", client_port))
                    s.send(str(reply).encode())
                    s.close()
                except:
                    pass
            else:
                threading.Thread(target=self.process_received_message, args=(received_message, waiting_time,)).start()

    def send(self, destination_node_id, message):
        """向指定节点发送消息"""
        destination_node_port = nodes_ports[destination_node_id]
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        host = socket.gethostname()
        try:
            s.connect((host, destination_node_port))
            s.send(message)
            print("node{id0}向node{id1}发送了一条消息{name}\n".format(id0=self.node_id,id1=destination_node_id,name=message))
            s.close()
        except :
            pass



    def broadcast_message(self, nodes_ids_list, message):  # Send to all connected nodes # Acts as a socket server
        for destination_node_id in nodes_ids_list:
            self.send(destination_node_id, message)

    # def broadcast_preprepare_message(self, request_message,
    #                                  nodes_ids_list):  # The primary node prepares and broadcats a PREPREPARE message
    #     """主节点广播pre-prepare消息"""
    #     if replied_requests[request_message["request"]] != 0:
    #         return
    #     with open(preprepare_format_file) as preprepare_format:
    #         preprepare_message = json.load(preprepare_format)
    #     preprepare_message["view_number"] = self.view_number
    #     global sequence_number
    #     preprepare_message["sequence_number"] = sequence_number
    #     preprepare_message["timestamp"] = request_message["timestamp"]
    #     tuple = (self.view_number, sequence_number)
    #     sequence_number = sequence_number + 1  # Increment the sequence number after each request在每次request发起后,序列号加一
    #     request = request_message["request"]
    #     digest = hashlib.sha256(request.encode()).hexdigest()
    #     preprepare_message["request_digest"] = digest
    #     preprepare_message["request"] = request_message["request"]
    #     preprepare_message["client_id"] = request_message["client_id"]
    #     self.preprepares[tuple] = digest
    #     self.message_log.append(preprepare_message)
    #
    #     preprepare_message = ecc.generate_sign(preprepare_message)
    #     print(str(preprepare_message))
    #     # 我TMD都已经给注释掉了,是谁放出的pre-prepare
    #     self.broadcast_message(nodes_ids_list, preprepare_message)
    #     # print("主节点{node}已经广播pre-prepare消息".format(node=self.primary_node_id))

    def broadcast_prepare_message(self, preprepare_message, nodes_ids_list):  # The node broadcasts a prepare message
        """广播prepare消息"""
        if replied_requests[preprepare_message["request"]] != 0:
            return
        with open(prepare_format_file) as prepare_format:
            prepare_message = json.load(prepare_format)
        prepare_message["view_number"] = self.view_number
        prepare_message["sequence_number"] = preprepare_message["sequence_number"]
        prepare_message["request_digest"] = preprepare_message["request_digest"]
        prepare_message["request"] = preprepare_message["request"]
        prepare_message["node_id"] = self.node_id
        prepare_message["client_id"] = preprepare_message["client_id"]
        prepare_message["timestamp"] = preprepare_message["timestamp"]

        # Generate a new random signing key

        prepare_message = ecc.generate_sign(prepare_message)
        self.broadcast_message(nodes_ids_list, prepare_message)

        return prepare_message

    def broadcast_commit_message(self, prepare_message, nodes_ids_list,
                                 sequence_number):  # The node broadcasts a commit message
        if replied_requests[prepare_message["request"]] != 0:
            return
        with open(commit_format_file) as commit_format:
            commit_message = json.load(commit_format)
        commit_message["view_number"] = self.view_number
        commit_message["sequence_number"] = sequence_number
        commit_message["node_id"] = self.node_id
        commit_message["client_id"] = prepare_message["client_id"]
        commit_message["request_digest"] = prepare_message["request_digest"]
        commit_message["request"] = prepare_message["request"]
        commit_message["timestamp"] = prepare_message["timestamp"]

        commit_message = ecc.generate_sign(commit_message)

        self.broadcast_message(nodes_ids_list, commit_message)

    def broadcast_view_change(self):  # The node broadcasts a view change
        """节点广播视图切换信息，后续可以在这里增加VRF信息"""
        with open(view_change_format_file) as view_change_format:
            view_change_message = json.load(view_change_format)
        new_view = self.view_number + 1
        view_change_message["new_view"] = new_view
        view_change_message["last_sequence_number"] = self.stable_checkpoint["sequence_number"]
        view_change_message["C"] = self.stable_checkpoint_validators
        view_change_message["node_id"] = self.node_id
        # 如果接收到的视图切换没有new_view,那么赋值l;如果有,那么直接append
        if new_view not in self.received_view_changes:
            self.received_view_changes[new_view] = [view_change_message]
        else:
            self.received_view_changes[new_view].append(view_change_message)

        # We define P as a set of prepared messages at the actual node with sequence number higher
        # than the sequence number in the last checkpoint
        # 将P定义为prepare信息的集合，在实际节点收到的prepare中的序列号高于最近的检查点的序列号时
        view_change_message["P"] = [message for message in self.prepared_messages if
                                    message["sequence_number"] > self.stable_checkpoint["sequence_number"]]

        # 将view_change_message附带签名然后广播出去
        view_change_message = ecc.generate_sign(view_change_message)
        list1 = []
        for node in the_nodes_ids_list:
            if node != self.node_id:
                list1.append(node)
        self.broadcast_message(list1, view_change_message)

        return view_change_message

    def send_reply_message_to_client(self, commit_message):
        """给client回复reply"""
        client_id = commit_message["client_id"]
        client_port = clients_ports[client_id]
        with open(reply_format_file) as reply_format:
            reply_message = json.load(reply_format)
        reply_message["view_number"] = self.view_number
        reply_message["client_id"] = client_id
        reply_message["node_id"] = self.node_id
        reply_message["timestamp"] = commit_message["timestamp"]
        reply = "Request executed"
        reply_message["result"] = reply
        reply_message["sequence_number"] = commit_message["sequence_number"]
        reply_message["request"] = commit_message["request"]
        reply_message["request_digest"] = commit_message["request_digest"]
        signed_reply_message = ecc.generate_sign(reply_message)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        host = socket.gethostname()
        try:
            s.connect((host, client_port))
            s.send(signed_reply_message)
            s.close()
            self.message_reply.append(reply_message)
        except:
            pass
        return reply


class HonestNode(Node):
    """诚实节点"""

    def receive(self, waiting_time=0):
        Node.receive(self, waiting_time)


class SlowNode(Node):
    """慢启动节点"""

    def receive(self, waiting_time=200):
        Node.receive(self, waiting_time)


class NonRespondingNode(Node):
    """无响应的节点:直接将节点的socket关闭掉,对接受的信息不做任何处理"""

    def receive(self, waiting_time=0):
        while True:
            s = self.socket
            sender_socket = s.accept()[0]
            received_message = sender_socket.recv(2048).decode()
            # print("Node %d got message: %s" % (self.node_id , received_message))
            # 直接将节点的socket关闭掉,对接受的信息不做任何处理
            sender_socket.close()
            # receives messages but doesn't do anything


class FaultyPrimary(Node):  # This node changes the client's request digest while sending a preprepare message
    """这个恶意的主节点在发送pre-prepare时,改变了client的request内的内容,篡改了digest"""

    def receive(self, waiting_time=0):
        """正常接收消息"""
        Node.receive(self, waiting_time)

    def broadcast_preprepare_message(self, request_message,
                                     nodes_ids_list):  # The primary node prepares and broadcats a PREPREPARE message
        """主节点广播pre-prepare消息.改变了request的哈希值"""
        if replied_requests[request_message["request"]] != 0:
            return
        with open(preprepare_format_file) as preprepare_format:
            preprepare_message = json.load(preprepare_format)
        preprepare_message["view_number"] = self.view_number
        global sequence_number
        preprepare_message["sequence_number"] = sequence_number
        preprepare_message["timestamp"] = request_message["timestamp"]
        tuple = (self.view_number, sequence_number)
        sequence_number = sequence_number + 1  # Increment the sequence number after each request在每次request发起后,序列号加一
        request = request_message["request"]+"abc"
        digest = hashlib.sha256(request.encode()).hexdigest()
        preprepare_message["request_digest"] = digest
        preprepare_message["request"] = request_message["request"]
        preprepare_message["client_id"] = request_message["client_id"]
        self.preprepares[tuple] = digest
        self.message_log.append(preprepare_message)

        preprepare_message = ecc.generate_sign(preprepare_message)
        self.broadcast_message(nodes_ids_list, preprepare_message)
        # print("主节点{node}已经广播pre-prepare消息".format(node=self.primary_node_id))

class FaultyNode(Node):  # This node changes digest in prepare message
    """这个恶意节点篡改了prepare的内容，篡改了digest"""

    def receive(self, waiting_time=0):
        Node.receive(self, waiting_time)

    def broadcast_prepare_message(self, preprepare_message, nodes_ids_list):  # The node broadcasts a prepare message
        if replied_requests[preprepare_message["request"]] != 0:
            return
        with open(prepare_format_file) as prepare_format:
            prepare_message = json.load(prepare_format)
        prepare_message["view_number"] = self.view_number
        prepare_message["sequence_number"] = preprepare_message["sequence_number"]
        prepare_message["request_digest"] = preprepare_message["request_digest"] + "abc"
        prepare_message["request"] = preprepare_message["request"]
        prepare_message["node_id"] = self.node_id
        prepare_message["client_id"] = preprepare_message["client_id"]
        prepare_message["timestamp"] = preprepare_message["timestamp"]

        prepare_message = ecc.generate_sign(prepare_message)

        self.broadcast_message(nodes_ids_list, prepare_message)

        return prepare_message



class FaultyRepliesNode(Node):  # This node sends a fauly reply to the client
    """这个节点发送一个错误的reply给client"""

    def receive(self, waiting_time=0):
        Node.receive(self, waiting_time)

    def send_reply_message_to_client(self, commit_message):
        client_id = commit_message["client_id"]
        client_port = clients_ports[client_id]
        with open(reply_format_file) as reply_format:
            reply_message = json.load(reply_format)
        reply_message["view_number"] = self.view_number
        reply_message["client_id"] = client_id
        reply_message["node_id"] = self.node_id
        reply_message["timestamp"] = commit_message["timestamp"]
        # 正确的reply应为reply execute,这里篡改了内容
        reply = "Faulty reply"
        reply_message["result"] = reply
        reply_message["sequence_number"] = commit_message["sequence_number"]
        reply_message["request"] = commit_message["request"]
        reply_message["request_digest"] = commit_message["request_digest"]

        signed_reply_message = ecc.generate_sign(reply_message)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        host = socket.gethostname()
        try:
            s.connect((host, client_port))
            s.send(signed_reply_message)
            s.close()
            self.message_reply.append(reply_message)
        except:
            pass
        return reply
