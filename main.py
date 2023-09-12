from client import *

import threading
import time

# 使用者要定义的参数
# 客户端重新发送request等待时间，这一次,将会向其他节点广播.用于client在时钟周期内没有收到任何一条的reply，这里取200
waiting_time_before_resending_request = 10
# (视图切换的时间限制??存疑),论文中没有给出固定值,这里取固定值120s
timer_limit_before_view_change = 5  # There is no value proposed in the paper so let's fix it to 120s
# 检查点协议多久执行一次?100是原论文的建议值
checkpoint_frequency = 100  # 100 is the proposed value in the original article

# # 这个部分没有写完吧?
# def type_node():
#     while True:
#         ecc.define_type_of_node(type_nodes)
#         global num_node
#         num_node = input()
#         if num_node in type_nodes:
#             num_node = int(num_node)
#             prepare()
#             break
#         else:
#             print("Sth wrong")
#         return num_node
#
#
# def prepare():
#     ecc.save_file('messages_formats/type_format.json', {})


# 定义了节点的类型,私以为这里不需要这么多类型,只需要诚实节点、宕机节点、恶意节点,三个足够了,type_nodes是枚举类型

type_nodes = {
    # 1:错误的主节点
    # 2:缓慢的节点
    # 3:诚实节点
    # 4:没有响应的节点(宕机？)
    # 5:故障节点
    # 6:返回错误故障的节点---恶意节点
    '1': 'faulty_primary',
    '2': 'slow_nodes',
    '3': 'honest_node',
    '4': 'non_responding_node',
    '5': 'faulty_node',
    '6': 'faulty_replies_node',
}
# 字典:在这里定义区块链网络的节点数量和类型,这里使用的是故障节点数量确定模型
# nodes = {
#     0: [
#         ("faulty_primary", 0),
#         ("slow_nodes", 0),
#         ("honest_node", 10),
#         ("non_responding_node", 0),
#         ("faulty_node", 0),
#         ("faulty_replies_node", 0),
#     ]
# }

nodes = {
    "faulty_primary": 1,
    "slow_nodes": 0,
    "honest_node": 3,
    "non_responding_node":  0,
    "faulty_node":  0,
    "faulty_replies_node": 0
}

# 启动PBFT共识算法
run_PBFT(nodes=nodes, proportion=1, checkpoint_frequency0=checkpoint_frequency, clients_ports0=clients_ports,
         timer_limit_before_view_change0=timer_limit_before_view_change)

# 暂停1秒再输出
time.sleep(1)

# 运行client
requests_number = 1
# 使用者选择他想仿真的request数量,他们都会在同一时间发送到Pbft网络当中，这里的request会被不同的client发送

# clients_list = []
# for i in range(requests_number):
#     # 初始化client对象
#     globals()["C%s" % str(i)] = Client(i, waiting_time_before_resending_request)
#     # 添加一个列表,该列表存储了所有的client对象
#     clients_list.append(globals()["C%s" % str(i)])
#
# for i in range(requests_number):
#     # 让client分别广播request,这里是让request发送到默认的第一个主节点0,如果0节点失效呢?该怎么办？
#     threading.Thread(target=clients_list[i].send_to_primary,
#                      args=("Requester  " + str(i), get_primary_id(), get_nodes_ids_list(), get_f())).start()
#     time.sleep(0)


clients_list = {}
for i in range(requests_number):
    # 初始化client对象
    clients_list["client" + str(i)] = Client(i, waiting_time_before_resending_request)


for i in range(requests_number):
    # 让client广播request给所有的节点
    threading.Thread(target=clients_list["client" + str(i)].send_request,
                     args=("Requester  " + str(i),  get_nodes_ids_list(), get_f())).start()
    time.sleep(0)

