#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <omp.h>
#include <string.h>

typedef struct {
    int threadid;
    int N;
    int NT;
    char pivotStrategy;
    //Shared arrays
    int *arr;
    int **thread_local_arr, **exchange_arr;
    int *local_sizes, *exchange_arr_sizes;
    int *pivots, *medians;
    //barriers
    pthread_barrier_t *bar_group, *bar_pair;
} thread_data;

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

void* global_sort(void * args){
    thread_data* t_args = (thread_data *) args;
    // Copying arguments to local variables
    int threadid = t_args->threadid;
    int N = t_args->N;
    int NT = t_args->NT;
    char pivotStrategy = t_args->pivotStrategy;
    //Shared arrays
    int *arr = t_args->arr;
    int **thread_local_arr = t_args->thread_local_arr;
    int *local_sizes = t_args->local_sizes;
    int **exchange_arr = t_args->exchange_arr;  
    int *exchange_arr_sizes = t_args->exchange_arr_sizes;  
    int *pivots = t_args->pivots;
    int *medians = t_args->medians;
    //Barriers
    pthread_barrier_t *bar_group = t_args->bar_group;
    pthread_barrier_t *bar_pair = t_args->bar_pair;

    //Allocate Chunks
    int chunksize = N / NT;
    int begin = threadid * chunksize;
    int end = (threadid == NT - 1)? N - 1 : (threadid + 1) * chunksize - 1;
    chunksize = end - begin + 1; //Not all thread's actual chunksize is N / NT

    //Copy Local Chunk
    int localsize = sizeof(int) * chunksize;
    int * local_arr = (int *) malloc(localsize);
    memcpy(local_arr, arr + begin, localsize);

    //Sort Local Chunk
    local_sort(local_arr, 0, chunksize - 1);

    int tpg = NT, gpi = 1;
    int group_barrier_id = 0, pair_barrier_id = 0;
    int localid, pairid, groupid;
    int pivot;

    while (tpg > 1){
        localid = threadid  % tpg;
        groupid = threadid / tpg;
        //Calculate Median
        if(chunksize !=0) medians[threadid] = local_arr[chunksize >> 1];

                
        //Only thread 1 does aggregation of medians
        if(localid == 0){
            if(pivotStrategy == 'c') {
                pivot = (medians[tpg >> 1] + medians[(tpg - 1) >> 1]) >> 1;
            }
            else if(pivotStrategy == 'b') {
                //Basically tpg threads from current thread since current thread is the first in a group
                int sum = 0;
                for(int i = threadid; i < threadid + tpg; i ++){
                    sum += medians[i];
                }
                pivot = sum / tpg;
            }
            else {
                printf("tpg = %d\n", tpg);
                pivot = medians[threadid];
            }
            //Broadcast Pivot
            for(int i = threadid; i < threadid + tpg; i ++){
                pivots[i] = pivot;
            }
        }
        //All threads in the group have to wait for getting their pivot
        pthread_barrier_wait(bar_group + group_barrier_id + groupid);
        pivot = pivots[threadid];

        int split = 0;
        while (split < chunksize && local_arr[split] < pivot) {
            split++;
        }

        printf("Thread %d-%d @ (%d ---- %d ---- %d)\n",threadid, tpg, begin, split, end);
        tpg = tpg >> 1; //Divide the number of threads/group by 2
        gpi = gpi << 1; //Multiply the number of groups/iterations by 2
        
        group_barrier_id += gpi; 
    }
}

int main(int ac, char* av[]){
    if (ac != 6){
        printf("Usage: ./%s N inputfile outputfile NT pivotStrategy\n", av[0]);
        return 1;
    }

    //Copy Command line arguments
    int N = atoi(av[1]);
    char* inputfile = av[2];
    char* outputfile = av[3];
    int NT = atoi(av[4]);
    char pivotStrategy = av[5][0];

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

    /**** PHASE 2: SORT NUMBERS ****/
    //Create barriers
    pthread_barrier_t *bar_group, *bar_pair;
    bar_pair = (pthread_barrier_t *) malloc(NT * sizeof(pthread_barrier_t)); //Create NT pair barriers
    for(int t = 0; t < NT; t++){
        pthread_barrier_init(&bar_pair[t], NULL, 2);
    }

    int bar_group_count = 0;
    for(int t = 1; t < NT; t = t << 1){
        bar_group_count += t;
    }
    bar_group = (pthread_barrier_t*) malloc(sizeof(pthread_barrier_t) * bar_group_count);
    int loop, bpg_index = 0;
    for (int t = 1; t < NT; t = t << 1) {
        for (loop = 0; loop < t; loop++) {
            pthread_barrier_init(&bar_group[bpg_index], NULL, NT / t);
            bpg_index++;
        }
    }

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
    for (int t = 0; t < NT; t++) {
        tdata[t].threadid = t;
        tdata[t].N = N;
        tdata[t].NT = NT;
        tdata[t].pivotStrategy = pivotStrategy;
        tdata[t].arr = arr;
        tdata[t].thread_local_arr = thread_local_arr;
        tdata[t].exchange_arr = exchange_arr;
        tdata[t].pivots = pivots;
        tdata[t].local_sizes = local_sizes;
        tdata[t].exchange_arr_sizes = exchange_arr_sizes;
        tdata[t].medians = medians;
        tdata[t].bar_group = bar_group;
        tdata[t].bar_pair = bar_pair;
        pthread_create(&threads[t], NULL, global_sort, (void *)&tdata[t]);
    }

    //Join Threads
    for (int t = 0; t < NT; t++) {
        pthread_join(threads[t], NULL);
    }

    //Free Shared Memory
    free(thread_local_arr); free(local_sizes);
    free(exchange_arr); free(exchange_arr_sizes);
    free(pivots); free(medians);
    free(tdata);
    
    //Destroy Barriers
    for(int t = 0; t < NT; t++) {
        pthread_barrier_destroy(&bar_pair[t]);
    }
    for(int t = 0; t < bar_group_count; t++) {
        pthread_barrier_destroy(&bar_group[t]);
    }

    //Free Barriers
    free(bar_group);
    free(bar_pair);
    
 
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
    return 0;
}