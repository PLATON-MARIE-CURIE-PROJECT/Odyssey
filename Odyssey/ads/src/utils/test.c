#include <stdio.h>

#include <mpi.h>

int comm_sz;
int my_rank;

int main(int argc, char **argv)
{

    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);


    if (my_rank == 0)
    {
        float data_to_sent[3] = {2, 12.2, -3}; // ok it is atomic
        printf("[SEND => WORKSTEALING - BSF - SHARING] Node %d send to node %d a new BSF .\n", my_rank, 1);
        printf("Data to send: [%f %f %f]\n", data_to_sent[0], data_to_sent[1], data_to_sent[2]);

        // MPI_Send(data_to_sent, 2, MPI_FLOAT, rank, WORKSTEALING_BSF_SHARE, communicators[my_rank]);
        MPI_Send(data_to_sent, 3, MPI_FLOAT, 1, 9003, MPI_COMM_WORLD);
    }
    else
    {
        float data_to_recv[3];
        MPI_Request req;
        int ready = 0;
        MPI_Irecv(data_to_recv, 3, MPI_FLOAT, 0, 9003, MPI_COMM_WORLD, &req);

        MPI_Test(&req, &ready, MPI_STATUS_IGNORE);

        while (!ready)
        {
            MPI_Test(&req, &ready, MPI_STATUS_IGNORE);
        }

        printf("Data received: [%f %f %f]\n", data_to_recv[0], data_to_recv[1], data_to_recv[2]);
    }
}