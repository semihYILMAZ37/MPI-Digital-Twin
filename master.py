from mpi4py import MPI
import sys

class Machine:
    def __init__(self, id, pid, initialOperation):
        self.id = id
        self.pid = pid
        self.initialOperation = initialOperation
        self.parent = None
        self.children = []
        self.initialProduct = None
    
    def add_child(self, child):
        self.children.append(child)
        self.children.sort()

    def set_product(self, product):
        self.initialProduct = product


def process_input(file_name):
    with open(file_name, 'r') as file:
        lines = file.readlines()

        num_machines = int(lines[0])
        num_cycles = int(lines[1])
        wear_factors = list(map(int, lines[2].split()))
        threshold = int(lines[3])

        machines = [] 
        line_idx = 4

        # initilization of machine objects by the information read from the input
        for _ in range(num_machines - 1):
            id, parentId, initialOperation = lines[line_idx].strip().split()
            id = int(id)
            parentId = int(parentId)
            machines.append(Machine(id, parentId, initialOperation))
            line_idx += 1

        idList = [machine.id for machine in machines]
        pidList = [machine.pid for machine in machines]

        #detection of root node
        for machine in machines:
            if machine.pid not in idList:
                root = Machine(machine.pid, None, None)
                break
        machines.insert(root.id, root)

        machines = {machine.id: machine for machine in machines}

        # adding children of machines to their corresponding field
        for machine in machines.values():
            if machine.pid:
                machines[machine.pid].add_child(machine.id)

        #detection of leaf nodes
        leafs = []
        for machine in machines.values():
            if machine.id not in pidList:
                leafs.append(machine)

        # sorting leaf nodes by id
        leafs.sort(key=lambda x: x.id)

        # setting products of leaf nodes from the input
        for leaf in leafs:
            leaf.set_product(lines[line_idx].strip())
            line_idx += 1

    return num_machines, machines, num_cycles, wear_factors, threshold

comm = MPI.COMM_WORLD
num_machines, machines, num_cycles, wear_factors, threshold = process_input(sys.argv[1])
# dynamic process creation
intercommunicator = comm.Spawn(sys.executable, args=['slave.py'], maxprocs=(num_machines+1))

# sending common info to node 0. (it will broadcast them to all.)
intercommunicator.send(threshold, dest=0, tag=105)
intercommunicator.send(num_cycles, dest=0, tag=106)
intercommunicator.send(wear_factors, dest=0, tag=107)

# sending special info to each machine
for i in range(1, num_machines+1):
    intercommunicator.send(machines[i].children, dest=i, tag=101)
    intercommunicator.send(machines[i].initialProduct, dest=i, tag=102)
    intercommunicator.send(machines[i].initialOperation, dest=i, tag=103)
    intercommunicator.send(machines[i].pid, dest=i, tag=104)

with open(sys.argv[2], 'w') as file:

    # receiving the results for each production cycle
    for i in range(num_cycles):
        result = intercommunicator.recv(tag=1)
        file.write(result + '\n')

    # receiving the maintenance logs from each machine
    for i in range(1, num_machines+1):
        # it first asks how many logs there are and then takes them.
        nofMaintenance = intercommunicator.recv(source=i,tag = 3)
        for j in range(nofMaintenance):
            req = intercommunicator.irecv(source=i,tag = 2)
            data = req.wait()
            file.write(data + '\n')
