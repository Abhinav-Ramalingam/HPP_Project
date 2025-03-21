#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <omp.h>
#include <string.h>


typedef struct {
    int myid;
    int N;
    int NT;
    char strat;
    int *arr;
    int **thread_local_arr, **exchange_arr;
    int *pivots, *local_sizes, *exchange_arr_sizes, *medians;
    pthread_barrier_t *bar_groups, *bar_pairs;
} thread_data;

void* global_sort(void *);
void local_sort(int *, int, int);

int main(int ac, char* av[]){
    if (ac != 6) {
        printf("Usage: ./%s N input output NT S\n", av[0]);
        printf("N - Number of Elements to Sort\n");
        printf("input - Input Numbers Dataset Filepath (generate with gen_data.c)\n");
        printf("output - Filepath to store the sorted elements\n");
        printf("NT - Number of Processors to Utilise\n");
        printf("S - Strategy to Select the pivot element:\n");
        printf("\t\'a\' - Pick Median of Processor-0 in Processor Group\n");
        printf("\t\'b\' - Select the mean of all medians in respective processor set\n");
        printf("\t\'c\' - Sort the medians and select the mean value of the two middlemost medians in each processor set\n");

        return 1;
    }
    int N = atoi(av[1]);
    char *inputfile = av[2];
    char *outputfile = av[3];
    int NT = atoi(av[4]);
    char strat = av[5][0];

    double start = omp_get_wtime(), stop;

    /**** PHASE 1: READ INPUT FROM FILE ****/

    int *arr = (int *)calloc(N, sizeof(int));
    if (arr == NULL) {
        perror("Memory allocation failed");
        return 1;
    }

    FILE *input_fp = fopen(inputfile, "r");
    if (input_fp == NULL) {
        perror("Failed to open input file");
        free(arr);
        return 1;
    }

    // Reading integers from the input file
    for (int i = 0; i < N; i++) {
        if (fscanf(input_fp, "%d", &arr[i]) != 1) {
            printf("Error reading number at index %d\n", i);
            break;
        }
    }
    fclose(input_fp);

    stop = omp_get_wtime();
    printf("Input time(s): %lf\n", stop - start);
    start = stop;

    /**** PHASE 2: SORT THE DATA ****/
    //Create Barriers
    int t;
    pthread_barrier_t *bar_pairs, *bar_groups;
    bar_pairs = (pthread_barrier_t*) malloc(sizeof(pthread_barrier_t) * NT);
    for (t = 0; t < NT; t++) {
        pthread_barrier_init(&bar_pairs[t], NULL, 2); 
    }
    int bar_groups_count = 0;
    for (t = 1; t < NT; t = t << 1) {
        bar_groups_count += t;
    }
    bar_groups = (pthread_barrier_t*) malloc(sizeof(pthread_barrier_t) * bar_groups_count);
    int loop, bpg_index = 0;
    for (t = 1; t < NT; t = t << 1) {
        for (loop = 0; loop < t; loop++) {
            pthread_barrier_init(&bar_groups[bpg_index], NULL, NT / t);
            bpg_index++;
        }
    }

    printf("Number of barriers = %d and %d", NT, bar_groups_count);

    //Create Shared Memory
    int NT_int_arr = sizeof(int*) * NT;
    int NT_int = sizeof(int) * NT;
    int ** thread_local_arr = (int **) (malloc(NT_int_arr));
    int * local_sizes = (int *) (malloc(NT_int));
    int ** exchange_arr = (int **) (malloc(NT_int_arr));
    int * exchange_arr_sizes = (int *) (malloc(NT_int));
    int * pivots = (int *) (malloc(NT_int));
    int * medians = (int *) (malloc(NT_int));

    //Create Threads
    pthread_t threads[NT];
    thread_data *tdata = (thread_data *)malloc(sizeof(thread_data) * NT);
    for (t = 0; t < NT; t++) {
        tdata[t].myid = t;
        tdata[t].N = N;
        tdata[t].NT = NT;
        tdata[t].strat = strat;
        tdata[t].arr = arr;
        tdata[t].thread_local_arr = thread_local_arr;
        tdata[t].exchange_arr = exchange_arr;
        tdata[t].pivots = pivots;
        tdata[t].local_sizes = local_sizes;
        tdata[t].exchange_arr_sizes = exchange_arr_sizes;
        tdata[t].medians = medians;
        tdata[t].bar_groups = bar_groups;
        tdata[t].bar_pairs = bar_pairs;
        pthread_create(&threads[t], NULL, global_sort, (void *)&tdata[t]);
    }

    //Join Threads
    for (t = 0; t < NT; t++) {
        pthread_join(threads[t], NULL);
    }

    //Free Shared Memory
    free(thread_local_arr); free(local_sizes);
    free(exchange_arr); free(exchange_arr_sizes);
    free(pivots); free(medians);
    
    //Destroy Barriers
    for(t = 0; t < NT; t++) {
        pthread_barrier_destroy(&bar_pairs[t]);
    }
    for(t = 0; t < bar_groups_count; t++) {
        pthread_barrier_destroy(&bar_groups[t]);
    }

    //Free Barriers
    free(bar_groups);
    free(bar_pairs);

    stop = omp_get_wtime();
    printf("Sorting time(s): %lf\n", stop - start);
    start = stop;

    /**** PHASE 3: WRITE OUTPUT TO FILE ****/
    FILE *output_fp = fopen(outputfile, "w");
    if (output_fp == NULL) {
        perror("Failed to open output file");
        free(arr);
        return 1;
    }

    for (int i = 0; i < N; i++) {
        fprintf(output_fp, "%d ", arr[i]);
    }
    fprintf(output_fp, "\n");
    fclose(output_fp);

    free(arr);

    stop = omp_get_wtime();
    printf("Output time(s): %lf\n", stop - start);

    return 0;
}

void* global_sort(void *args){
    thread_data* t_args =  (thread_data *) args;

    // Copying arguments to local variables
    int myid = t_args->myid;
    int N = t_args->N;
    int NT = t_args->NT;
    char strat = t_args->strat;
    int *arr = t_args->arr;

    int **thread_local_arr = t_args->thread_local_arr;
    int *local_sizes = t_args->local_sizes;

    int **exchange_arr = t_args->exchange_arr;  
    int *exchange_arr_sizes = t_args->exchange_arr_sizes;  
    
    int *pivots = t_args->pivots;
    int *medians = t_args->medians;

    pthread_barrier_t *bar_groups = t_args->bar_groups;
    pthread_barrier_t *bar_pairs = t_args->bar_pairs;

    int chunk_size = N / NT;
    int begin = myid * chunk_size;
    int end = (myid == NT - 1) ? N - 1 : (myid + 1) * chunk_size - 1;
    chunk_size = end - begin + 1; //Not all thread's actual chunk_size is N / NT

    //Create a local array of chunk_size in this thread executing the global_sort
    int local_data = chunk_size * sizeof(int);
    int * local_arr = (int *) (malloc(local_data));
    thread_local_arr[myid] = local_arr;
    memcpy(local_arr, arr + begin, local_data);
    
    //local sort on LOCAL array
    local_sort(local_arr, 0, chunk_size - 1);
    //printf("\nSuccessfully localsort %d\n", myid);

    int localid, groupid, exchangeid; //Identifier of thread within a group and, group it belongs to, identifier of partner
    int tpg = NT, gpi = 1;  //threads per group, groups per iteration
    int group_barrier_id = 0, pair_barrier_id = 0; //Idenfier of which barrier is being waited on

    medians[myid] = 0;
    int *merged_arr; //local_arr + exchange_arr with the correct halves

    while (tpg > 1){
        // Median Calculation
        if (chunk_size > 0) {
            medians[myid] = local_arr[chunk_size / 2]; 
        }

        //Pivot Calculation
        int pivot;

        localid = myid % tpg;
        groupid = myid / tpg;
        //printf("Thread %d -- calculated median\n", myid);
        pthread_barrier_wait(bar_groups + group_barrier_id + groupid);
        if (localid == 0) {
            switch(strat){
                case 'a':
                    //Pick Median of Processor-0 in Processor Group
                    pivot = medians[myid];
                    break;
                case 'b':
                    //Select the mean of all medians in respective processor set
                    int sum = 0;
                    for(int i = 0; i < tpg; i++){
                        sum += medians[myid + i];
                    }
                    pivot = sum / tpg;
                    break;
                case 'c':
                    //Sort the medians and select the mean value of the two middlemost medians in each processor set
                    local_sort(medians, myid, myid + tpg - 1);
                    pivot = (medians[myid + (tpg >> 1) - 1] + medians[myid + (tpg >> 1)]) >> 1;

                    break;
                default:
                    pivot = medians[myid];
                    break;
            }
            //Thread with local-id Zero will always aggregate and broadcast
            for (int i = 0; i < tpg; i ++){
                pivots[myid + i] = pivot;
            }
        }
        //printf("Thread %d -- calculated pivot\n", myid);
        pthread_barrier_wait(bar_groups + group_barrier_id + groupid);
        pivot = pivots[myid];

        //Splitpoint calculation
        int split = 0;
        while (split < chunk_size && local_arr[split] < pivot) {
            split++;
        }
        //printf("Thread %d(%d) == Chunk %d: Split %d\n", myid, tpg, chunk_size, split);
        //printf("Start = %d, End = %d\nThreadid is myid=%d: %d:%d = pivot: %d @ %d: ", begin, end, myid, groupid, localid, pivot, split);
        pthread_barrier_wait(bar_groups + group_barrier_id + groupid);


        //Sending over the right halves to corresponding exchange partner
        int local_arr_index, local_arr_size;
        if(localid < tpg >> 1){
            //Threads in the 'lower half' would exchange elements greater than the pivot with their exchange partner
            exchangeid = myid + (tpg >> 1);
            local_arr_index = 0; local_arr_size = split;
            exchange_arr[myid] = local_arr + split; exchange_arr_sizes[myid] = chunk_size - split;
            pair_barrier_id = exchangeid;
        }
        else {
            //Threads in the 'upper half' would exchange elements less than the pivot with their exchange partner
            exchangeid = myid - (tpg >> 1);
            local_arr_index = split; local_arr_size = chunk_size - split;
            exchange_arr[myid] = local_arr; exchange_arr_sizes[myid] = split; 
            pair_barrier_id = myid; //Here, we are always using the barrier of the upper half thread as convention
        }

        //printf("Thread %d -- sent data\n", myid);
        pthread_barrier_wait(bar_pairs + pair_barrier_id);

        //Calculating the new chunk_size after exchange
        chunk_size = local_arr_size + exchange_arr_sizes[exchangeid];
        merged_arr = (int *) (malloc(sizeof(int) * chunk_size));
        
        int i = 0, j = 0, k = 0;
        int *local_arr_ptr = local_arr + local_arr_index;  // Pointer to the starting element of local_arr

        printf("Thread %d-%d -- merging\n", myid, exchangeid);
        while (j < local_arr_size && k < exchange_arr_sizes[exchangeid]) {
            if (local_arr_ptr[j] < exchange_arr[exchangeid][k]) {
                merged_arr[i++] = local_arr_ptr[j++];
            } else {
                merged_arr[i++] = exchange_arr[exchangeid][k++];
            }
        }

        while (j < local_arr_size) {
            merged_arr[i++] = local_arr_ptr[j++];
        }

        while (k < exchange_arr_sizes[exchangeid]) {
            merged_arr[i++] = exchange_arr[exchangeid][k++];
        }

        printf("Thread %d-%d reached this barrier %d\n", myid, exchangeid, pair_barrier_id);
        //printf("Thread %d -- merged\n", myid);
        pthread_barrier_wait(bar_pairs + pair_barrier_id);
        //printf("Thread %d(%d) crossed this barrier %d\n", myid, exchangeid, pair_barrier_id);

        free(local_arr);
        printf("Thread %d-%d -- freed\n", myid, exchangeid);
        local_arr = merged_arr;
        thread_local_arr[myid] = local_arr; //Copy back the modified local_arr;

        tpg = tpg >> 1; //Divide the number of threads/group by 2
        gpi = gpi << 1; //Multiply the number of groups/iterations by 2

        group_barrier_id += gpi; //Move on to using the next set of barriers for groups of threads
    }
    //printf("\nSuccessfully while %d\n", myid);

    //Final chunk_size of the local_arr after all the swaps
    local_sizes[myid] = chunk_size;
    
    if (NT != 1) {
        //meaning there were multiple threads
        pthread_barrier_wait(bar_groups);
    }

    //Find the global location of the sorted local array = cumulative sizes of local arrays before current thread's local array
    int global_loc = 0;
    for (int i = 0; i < myid; i++){
        global_loc += local_sizes[i];
    }


    
    memcpy(arr + global_loc, local_arr, sizeof(int) * chunk_size);

    free(local_arr);

    return NULL;

}

// Standard quicksort function
void local_sort(int *arr, int begin, int end) {
    if (begin >= end) return;

    int pivot = arr[end]; 
    int i = begin - 1;

    for (int j = begin; j < end; j++) {
        if (arr[j] <= pivot) {
            i++;
            int temp = arr[i];
            arr[i] = arr[j];
            arr[j] = temp;
        }
    }

    int temp = arr[i + 1];
    arr[i + 1] = arr[end];
    arr[end] = temp;

    local_sort(arr, begin, i);
    local_sort(arr, i + 2, end);
}