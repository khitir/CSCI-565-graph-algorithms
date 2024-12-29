import heapq
from collections import deque
from enum import Enum, auto

# Define constants and enums for states and message types
class NodeState(Enum):
    SLEEPING = auto()
    FIND = auto()
    FOUND = auto()

class EdgeState(Enum):
    BASIC = auto()
    BRANCH = auto()
    REJECTED = auto()

class MessageType(Enum):
    CONNECT = auto()
    INITIATE = auto()
    TEST = auto()
    ACCEPT = auto()
    REJECT = auto()
    REPORT = auto()
    CHANGE_ROOT = auto()
    WAKEUP = auto()

class Message:
    def __init__(self, sender, message_type, **kwargs):
        self.sender = sender
        self.type = message_type
        self.__dict__.update(kwargs)

class Edge:
    def __init__(self, node1, node2, weight):
        self.nodes = (node1, node2)
        self.weight = weight
        self.state = EdgeState.BASIC

    def other_node(self, node):
        return self.nodes[1] if self.nodes[0] == node else self.nodes[0]

class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.edges = []  # List of edges connected to this node
        self.state = NodeState.SLEEPING
        self.level = None
        self.fragment_name = None
        self.find_count = 0
        self.in_branch = None
        self.best_edge = None
        self.best_weight = float('inf')
        self.test_edge = None
        self.message_queue = deque()

    def add_edge(self, edge):
        self.edges.append(edge)

    def send_message(self, target_node, message):
        print(f"Node {self.node_id} sending message {message.type} to Node {target_node.node_id}")
        target_node.message_queue.append(message)

    def wakeup(self):
        # Procedure wakeup
        min_edge = min(self.edges, key=lambda e: e.weight)
        min_edge.state = EdgeState.BRANCH
        self.level = 0
        self.state = NodeState.FOUND
        self.find_count = 0
        self.in_branch = min_edge
        other_node = min_edge.other_node(self)
        # Send Connect(0) message over min_edge
        message = Message(sender=self, message_type=MessageType.CONNECT, level=0)
        self.send_message(other_node, message)

    def process_message(self, message):
        # Avoid processing the same message repeatedly
        if message.sender is not None and (self.node_id, message.sender.node_id, message.type) in processed_reports:
            return
        if message.sender is not None:
            processed_reports.add((self.node_id, message.sender.node_id, message.type))
        print(f"Node {self.node_id} processing message {message.type} from Node {message.sender.node_id if message.sender else 'None'}")
        if message.type == MessageType.WAKEUP:
            if self.state == NodeState.SLEEPING:
                self.wakeup()
        elif message.type == MessageType.CONNECT:
            self.process_connect(message)
        elif message.type == MessageType.INITIATE:
            self.process_initiate(message)
        elif message.type == MessageType.TEST:
            self.process_test(message)
        elif message.type == MessageType.ACCEPT:
            self.process_accept(message)
        elif message.type == MessageType.REJECT:
            self.process_reject(message)
        elif message.type == MessageType.REPORT:
            self.process_report(message)
        elif message.type == MessageType.CHANGE_ROOT:
            self.process_change_root(message)

    def process_connect(self, message):
        edge = self.get_edge_to(message.sender)
        if self.state == NodeState.SLEEPING:
            self.wakeup()
        if message.level < self.level:
            edge.state = EdgeState.BRANCH
            # Send Initiate(LN, FN, SN) on edge
            initiate_message = Message(sender=self, message_type=MessageType.INITIATE,
                                       level=self.level, fragment_name=self.fragment_name,
                                       state=self.state)
            self.send_message(message.sender, initiate_message)
        elif edge.state == EdgeState.BASIC:
            # Delay processing
            if message not in self.message_queue and (self.node_id, message.sender.node_id, message.type) not in processed_reports:
                self.message_queue.append(message)
        else:
            # Send Initiate(LN+1, w(j), Find) on edge
            edge.state = EdgeState.BRANCH
            self.level += 1
            self.fragment_name = edge.weight
            self.state = NodeState.FIND
            initiate_message = Message(sender=self, message_type=MessageType.INITIATE,
                                       level=self.level, fragment_name=self.fragment_name,
                                       state=self.state)
            self.send_message(message.sender, initiate_message)

    def process_initiate(self, message):
        if self.level == message.level and self.fragment_name == message.fragment_name and self.state == message.state:
            # Avoid processing the same INITIATE message repeatedly
            return
        self.level = message.level
        self.fragment_name = message.fragment_name
        self.state = message.state
        self.in_branch = self.get_edge_to(message.sender)
        self.best_edge = None
        self.best_weight = float('inf')
        # Send Initiate(L, F, S) on all edges in state BRANCH except in_branch
        for edge in self.edges:
            if edge != self.in_branch and edge.state == EdgeState.BRANCH:
                initiate_message = Message(sender=self, message_type=MessageType.INITIATE,
                                           level=self.level, fragment_name=self.fragment_name,
                                           state=self.state)
                if initiate_message not in self.message_queue:
                    self.send_message(edge.other_node(self), initiate_message)
        if self.state == NodeState.FIND:
            self.find_count = len([edge for edge in self.edges if edge.state == EdgeState.BRANCH and edge != self.in_branch])
            self.test()

    def test(self):
        # Procedure test
        basic_edges = [edge for edge in self.edges if edge.state == EdgeState.BASIC]
        if basic_edges:
            min_edge = min(basic_edges, key=lambda e: e.weight)
            self.test_edge = min_edge
            test_message = Message(sender=self, message_type=MessageType.TEST,
                                   level=self.level, fragment_name=self.fragment_name)
            self.send_message(min_edge.other_node(self), test_message)
        else:
            self.test_edge = None
            self.report()

    def process_test(self, message):
        processed_reports.add((self.node_id, message.sender.node_id, message.type))
        if self.state == NodeState.SLEEPING:
            self.wakeup()
        if message.level > self.level:
            # Delay processing, but prevent repeated processing
            if message not in self.message_queue and (self.node_id, message.sender.node_id, message.type) not in processed_reports:
                self.message_queue.append(message)
        elif message.fragment_name != self.fragment_name:
            # Send ACCEPT
            accept_message = Message(sender=self, message_type=MessageType.ACCEPT)
            self.send_message(message.sender, accept_message)
        else:
            # Edge is internal; reject it
            edge = self.get_edge_to(message.sender)
            if edge.state == EdgeState.BASIC:
                edge.state = EdgeState.REJECTED
            if self.test_edge != edge:
                reject_message = Message(sender=self, message_type=MessageType.REJECT)
                self.send_message(message.sender, reject_message)
            else:
                # Mark the current TEST as processed to avoid looping
                processed_reports.add((self.node_id, message.sender.node_id, message.type))
                self.test()
    
    def process_accept(self, message):
        edge = self.get_edge_to(message.sender)
        self.test_edge = None
        if edge.weight < self.best_weight:
            self.best_edge = edge
            self.best_weight = edge.weight
        self.report()
    
    def process_reject(self, message):
        edge = self.get_edge_to(message.sender)
        if edge.state == EdgeState.BASIC:
            edge.state = EdgeState.REJECTED
        self.test()
    
    def report(self):
        if self.find_count == 0 and self.test_edge is None:
            if self.state != NodeState.FOUND:
                self.state = NodeState.FOUND
                print(f"Node {self.node_id} has reached FOUND state.")
            if self.in_branch:
                report_message = Message(sender=self, message_type=MessageType.REPORT, weight=self.best_weight)
                self.send_message(self.in_branch.other_node(self), report_message)
        if self.find_count == 0 and self.test_edge is None:
            if self.state != NodeState.FOUND:
                self.state = NodeState.FOUND
                print(f"Node {self.node_id} has reached FOUND state.")
                self.state = NodeState.FOUND
                print(f"Node {self.node_id} has reached FOUND state.")
            if self.in_branch:
                report_message = Message(sender=self, message_type=MessageType.REPORT, weight=self.best_weight)
                self.send_message(self.in_branch.other_node(self), report_message)
    
    def process_report(self, message):
        if message.sender != self.in_branch.other_node(self):
            if self.find_count > 0:
                self.find_count -= 1
            if message.weight < self.best_weight:
                self.best_weight = message.weight
                self.best_edge = self.get_edge_to(message.sender)
            self.report()
        else:
            if self.state == NodeState.FIND:
                if message not in self.message_queue and (self.node_id, message.sender.node_id, message.type) not in processed_reports:
                    self.message_queue.append(message)
            elif message.weight >= self.best_weight:
                self.change_root()
            elif message.weight == float('inf'):
                # Algorithm completed
                if self.state != NodeState.FOUND:
                    print(f"Node {self.node_id}: MST has been found.")
                    self.state = NodeState.FOUND
        if (self.node_id, message.sender.node_id, message.type) in processed_reports:
            return
        processed_reports.add((self.node_id, message.sender.node_id, message.type))
        if message.sender != self.in_branch.other_node(self):
            if (self.node_id, message.sender.node_id, message.type) in processed_reports:
                return
            if self.find_count > 0:
                self.find_count -= 1
            if message.weight < self.best_weight:
                self.best_weight = message.weight
                self.best_edge = self.get_edge_to(message.sender)
            self.report()
        else:
            if self.state == NodeState.FIND:
                if message not in self.message_queue and (self.node_id, message.sender.node_id, message.type) not in processed_reports:
                    self.message_queue.append(message)
            elif message.weight > self.best_weight:
                self.change_root()
            elif message.weight == float('inf'):
                # Algorithm completed
                if self.state != NodeState.FOUND:
                    print(f"Node {self.node_id}: MST has been found.")
                    self.state = NodeState.FOUND
            else:
                pass

    def change_root(self):
        if self.best_edge and self.best_edge.state != EdgeState.BRANCH:
            self.best_edge.state = EdgeState.BRANCH
            connect_message = Message(sender=self, message_type=MessageType.CONNECT, level=self.level)
            self.send_message(self.best_edge.other_node(self), connect_message)
        if self.best_edge and self.best_edge.state == EdgeState.BRANCH:
            change_root_message = Message(sender=self, message_type=MessageType.CHANGE_ROOT)
            self.send_message(self.best_edge.other_node(self), change_root_message)
        elif self.best_edge:
            connect_message = Message(sender=self, message_type=MessageType.CONNECT, level=self.level)
            self.best_edge.state = EdgeState.BRANCH
            self.send_message(self.best_edge.other_node(self), connect_message)

    def process_change_root(self, message):
        self.change_root()

    def get_edge_to(self, node):
        for edge in self.edges:
            if node in edge.nodes:
                return edge
        return None

    def run(self):
        global processed_reports
        # Process messages in the queue
        while self.message_queue:
            message = self.message_queue.popleft()
            self.process_message(message)

def simulate(nodes):
    global processed_reports
    processed_reports = set()
    # Keep processing messages until all queues are empty
    while any(node.message_queue for node in nodes.values()):
        for node in nodes.values():
            node.run()

def main():
    # Read edges from file
    edges_info = []
    with open(r"C:\Users\Zakaria\Desktop\a1\fb-origin-with-unique-weights.txt", "r") as file:
        for line in file:
            parts = line.strip().split()
            if len(parts) != 3:
                continue  # Skip invalid lines
            n1, n2, weight = map(int, parts)
            edges_info.append((n1, n2, weight))

    # Create nodes dynamically based on edges_info
    nodes = {}
    node_ids = {n for edge in edges_info for n in edge[:2]}
    for i in node_ids:
        nodes[i] = Node(node_id=i)

    # Create edges dynamically based on edges_info
    edges = []
    for n1, n2, weight in edges_info:
        edge = Edge(nodes[n1], nodes[n2], weight)
        nodes[n1].add_edge(edge)
        nodes[n2].add_edge(edge)
        edges.append(edge)

    # Wake up all nodes to start the algorithm
    for node in nodes.values():
        node.message_queue.append(Message(sender=None, message_type=MessageType.WAKEUP))

    # Simulate the algorithm
    simulate(nodes)

    # Output the MST edges
    mst_edges = []
    for edge in edges:
        if edge.state == EdgeState.BRANCH:
            mst_edges.append((edge.nodes[0].node_id, edge.nodes[1].node_id, edge.weight))

    print("\nMinimum Spanning Tree edges:")
    total_weight = 0
    for n1, n2, weight in sorted(mst_edges):
        print(f"Edge ({n1}, {n2}) with weight {weight}")
        total_weight += weight

    print(f"Total weight of the MST: {total_weight}")

    # Write the MST to an output file
    output_file = r"C:\Users\Zakaria\Desktop\a1\mst_GHS_output.txt"
    write_mst_to_file(output_file, mst_edges, total_weight)

def write_mst_to_file(output_file, mst_edges, mst_weight):
    with open(output_file, 'w') as file:
        for n1, n2, weight in sorted(mst_edges):
            file.write(f"Edge ({n1}, {n2}) with weight {weight}\n")
        file.write(f"Total weight of the MST: {mst_weight}\n")

if __name__ == "__main__":
    main()