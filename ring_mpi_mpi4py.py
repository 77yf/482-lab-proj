"""
Problem 2 – MPI Ring Message Passing using mpi4py
Run with: mpiexec -n 5 python ring_mpi_mpi4py.py

Each process sends a message to its successor and receives from its predecessor.
"""

from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

successor   = (rank + 1) % size
predecessor = (rank - 1 + size) % size

send_msg = f"Hello from Process {rank}"

if rank % 2 == 0:
    comm.send(send_msg, dest=successor,   tag=0)
    recv_msg = comm.recv(source=predecessor, tag=0)
else:
    recv_msg = comm.recv(source=predecessor, tag=0)
    comm.send(send_msg, dest=successor,   tag=0)

print(f"Process {rank} | sent to P{successor}: \"{send_msg}\" | received from P{predecessor}: \"{recv_msg}\"")

MPI.Finalize()