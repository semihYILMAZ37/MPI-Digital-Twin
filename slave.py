from mpi4py import MPI

comm = MPI.COMM_WORLD
intercommunicator = comm.Get_parent()

def enhance(input):
    return input[0] + input + input[-1]

def reverse(input):
    return input[::-1]

def chop(input):
    if len(input) > 1:
        return input[:-1]
    else:
        return input
    
def trim(input):
    if len(input) > 2:
        return input[1:-1]
    else:
        return input
    
def split(input):
    if len(input) > 1:
        if len(input) % 2 == 0:
            return input[:(len(input)//2)]
        else:
            return input[:(len(input)//2)+1]
    else:
        return input

def add(inputs):
    return "".join(inputs)


def even_operation(state,input):
    # enhance, split, chop
    if state == 0:
        return enhance(input)
    elif state == 1:
        return split(input)
    elif state == 2:
        return chop(input)

def odd_operation(state,input):
    # reverse, trim
    if state == 0:
        return reverse(input)
    elif state == 1:
        return trim(input)

if comm.rank == 0:
    # rank 0 process receives the shared information from the control room and broadcasts them
    threshold = intercommunicator.recv(tag=105)
    num_cycles = intercommunicator.recv(tag=106)
    wear_factor = intercommunicator.recv(tag=107)
    threshold = comm.bcast(threshold, root=0)
    num_cycles = comm.bcast(num_cycles, root=0)
    wear_factor = comm.bcast(wear_factor, root=0)
else:
    rank = comm.rank
    threshold = None
    num_cycles = None
    wear_factor = None
    threshold = comm.bcast(threshold, root=0)
    num_cycles = comm.bcast(num_cycles, root=0)
    wear_factor = comm.bcast(wear_factor, root=0)
    children = intercommunicator.recv(tag=101)
    initialProduct = intercommunicator.recv(tag=102)
    initialOperation = intercommunicator.recv(tag=103)
    parentID = intercommunicator.recv(tag=104)
 
    nofMaintenance = 0
    #terminal node
    if parentID == None:
        for i in range(num_cycles):
            inputProducts = []
            for i in children:
                inputProducts.append(comm.recv(source=i, tag=1))
            result = add(inputProducts)
            # blocking send of the final product to the control room
            intercommunicator.send(result, dest=0, tag=1)
    #non terminal nodes
    else:
        if rank % 2 == 0: #even ids
            wear_factors = [wear_factor[0], wear_factor[4], wear_factor[2]]
            if initialOperation == "enhance":
                state = 0
            elif initialOperation == "split":
                state = 1
            elif initialOperation == "chop":
                state = 2
        else: # odd ids
            wear_factors = [wear_factor[1], wear_factor[3]]
            if initialOperation == "reverse":
                state = 0
            elif initialOperation == "trim":
                state = 1

        accumulated_wear = 0
        for cycle in range(1,num_cycles+1):
            #if not a leaf node, then waits for children to receive their products
            if initialProduct == None:
                inputProducts = []
                for i in children:
                    inputProducts.append(comm.recv(source=i, tag=1))
                inputProduct = add(inputProducts)
            # if leaf node, then it already has its input
            else:
                inputProduct = initialProduct
            # operation
            if rank % 2 == 0:
                product = even_operation(state, inputProduct)
            else:
                product = odd_operation(state, inputProduct)
            # maintenance calculation
            accumulated_wear += wear_factors[state]
            if accumulated_wear >= threshold:
                nofMaintenance += 1
                cost = (accumulated_wear - threshold + 1) * wear_factors[state]
                # non-blocking send of maintenance logs to main control room
                req = intercommunicator.isend(f"{rank}-{cost}-{cycle}", dest=0, tag=2)
                req.wait()
                accumulated_wear = 0
            #state update
            state = (state + 1) % len(wear_factors)
            # blocking send of the product to the parent node
            comm.send(product, dest=parentID, tag=1)
    # blocking send of the number of maintenance needed to the main control room
    intercommunicator.send(nofMaintenance, dest=0, tag=3)

