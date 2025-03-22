#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <time.h>

typedef struct thread_const_data_t {
    int   N;
    int NT;
    char  strat;
    int **thread_local_arr, **exchange_arr;
    int *arr, *exchange_arr_sizes, *pivots, *local_sizes, *medians;
    pthread_barrier_t *bar_pair, *bar_group;
} thread_const_data_t;

typedef struct {
    int threadid;
    thread_const_data_t* t_const_args;
} thread_data_t;

void* global_sort(void *);
void local_sort(int *, int, int);
double get_time();

int main(int ac, char** av) {
    if (ac != 6) {
        printf("Usage: ./%s N input output NT S\n", av[0]);
        printf("N - Number of Elements to Sort\n");
        printf("input - Input Numbers Dataset Filepath (generate with gen_data.c)\n");
        printf("output - Filepath to store the sorted elements\n");
        printf("NT - Number of Processors to Utilise\n");
        printf("strat - Strategy to Select the pivot element:\n");
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

    double start = get_time(), stop;

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
        if (fscanf(input_fp, "%d", (arr + i)) != 1) {
            printf("Error reading number at index %d\n", i);
            break;
        }
    }
    fclose(input_fp);

    stop = get_time();
    printf("Input time(s): %lf\n", stop - start);
    start = stop;

    /**** PHASE 2: SORT INPUT ****/
    //Create Barriers
    int t = 0;
    int bar_group_count = NT - 1;
    pthread_barrier_t bar_pair[NT], bar_group[bar_group_count]; 
    for (t = 0; t < NT; t++) {
        pthread_barrier_init(bar_pair + t, NULL, 2);
    }
    
    int loop, bpg_index = 0;
    for (t = 1; t < NT; t <<= 1) {
        for (loop = 0; loop < t; loop++) {
            pthread_barrier_init(bar_group + bpg_index, NULL, NT / t);
            bpg_index++;
        }
    }

    //Create Shared Memory
    int NT_int_arr = NT * sizeof(int*);
    int NT_int = NT * sizeof(int);
    int ** thread_local_arr = (int **) (malloc(NT_int_arr));
    int * local_sizes = (int *) (malloc(NT_int));
    int ** exchange_arr = (int **) (malloc(NT_int_arr));
    int * exchange_arr_sizes = (int *) (malloc(NT_int));
    int * pivots = (int *) (malloc(NT_int));
    int * medians = (int *) (malloc(NT_int));

    //Create Threads
    pthread_t threads[NT];
    thread_const_data_t t_const_args = {
        N, NT, strat, 
        thread_local_arr, exchange_arr, 
        arr, 
        exchange_arr_sizes, pivots, local_sizes, medians, 
        bar_pair, bar_group
    };
    for (t = 0; t < NT; t++) {
        thread_data_t* t_args = (thread_data_t*) malloc(sizeof(thread_data_t));
        t_args->threadid = t;
        t_args->t_const_args = &t_const_args;
        pthread_create(threads + t, NULL, global_sort, (void*) t_args);
    }
    
    //Join Threads
    for (int i = 0; i < NT; i++)
        pthread_join(threads[i], NULL);
    
    //Free Shared Memory
    free(thread_local_arr); free(local_sizes);
    free(exchange_arr); free(exchange_arr_sizes);
    free(pivots); free(medians);

    //Destroy Barriers
    for(t = 0; t < NT; t++) {
        pthread_barrier_destroy(bar_pair + t);
    }
    for(t = 0; t < bar_group_count; t++) {
        pthread_barrier_destroy(bar_group + t);
    }

    stop = get_time();
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

    stop = get_time();
    printf("Output time(s): %lf\n", stop - start);
    
    return 0;
}

void* global_sort(void* t_args) {
    const thread_data_t* args   = (thread_data_t*) t_args;

    //Copy arguments to local variables
    const int threadid    = args->threadid;      
    const thread_const_data_t* s_args = args->t_const_args;
    const int   N      = s_args->N;     
    const int NT      = s_args->NT;       
    const char  strat      = s_args->strat;      
    int*  arr   = s_args->arr;  

    int** thread_local_arr  = s_args->thread_local_arr;
    int*  local_sizes   = s_args->local_sizes; 

    int** exchange_arr  = s_args->thread_local_arr; 
    int*  exchange_arr_sizes  = s_args->exchange_arr_sizes; 
    
    int*  pivots   = s_args->pivots;  
    int*  medians   = s_args->medians;

    pthread_barrier_t* bar_pair = s_args->bar_pair; 
    pthread_barrier_t* bar_group = s_args->bar_group;

    int chunk_size = N >> __builtin_ctz(NT);;
    int begin = threadid * chunk_size;
    int end = threadid < NT - 1 ? (threadid + 1) * chunk_size - 1 : N - 1;
    chunk_size = end - begin + 1; //Not all thread's actual chunk_size is N / NT

    //Create a local array of chunk_size in this thread executing the global_sort
    int local_size = chunk_size * sizeof(int);
    int* local_arr  = (int*) malloc(local_size);
    thread_local_arr[threadid] = local_arr;
    memcpy(local_arr, arr + begin, local_size);

    //local sort on LOCAL array
    local_sort(local_arr, 0, chunk_size - 1);

    int localid, groupid, exchangeid; //Identifier of thread within a group and, group it belongs to, identifier of partner
    int tpg = NT, gpi = 1; //threads per group and groups per iteration
    int group_barrier_id = 0, pair_barrier_id = 0; //Idenfier of which barrier is being waited on

    medians[threadid] = 0;
    int* merged_arr;
      
    while (tpg > 1) {
        //Median Calculation
        if (chunk_size != 0)  medians[threadid] = local_arr[chunk_size >> 1];

        //Pivot Calculation
        int pivot;             
        
        localid = threadid % tpg;
        groupid = threadid / tpg;
        pthread_barrier_wait(bar_group + group_barrier_id + groupid);
        if (localid == 0) {
            switch(strat){
                case 'a':
                    //Pick Median of Processor-0 in Processor Group
                    pivot = medians[threadid];
                    break;
                case 'b':
                    //Select the mean of all medians in respective processor set
                    int sum = 0;
                    for(int i = 0; i < tpg; i++){
                        sum += medians[threadid + i];
                    }
                    pivot = sum / tpg;
                    break;
                case 'c':
                    //Sort the medians and select the mean value of the two middlemost medians in each processor set
                    local_sort(medians, threadid, threadid + tpg - 1);
                    pivot = (medians[threadid + (tpg >> 1) - 1] + medians[threadid + (tpg >> 1)]) >> 1;

                    break;
                default:
                    pivot = medians[threadid];
                    break;
            }
            //Thread with local-id Zero will always aggregate and broadcast
            for (int i = 0; i < tpg; i ++){
                pivots[threadid + i] = pivot;
            }
        }
        pthread_barrier_wait(bar_group + group_barrier_id + groupid);

        //Splitpoint calculation
        pivot = pivots[threadid];
        int split = 0;
        while (split < chunk_size && local_arr[split] <= pivot) {
            split++;
        }

        pthread_barrier_wait(bar_group + group_barrier_id + groupid);

        int local_arr_size, local_arr_index;          
        if (localid < tpg >> 1) {
            //Threads in the 'lower half' would exchange elements greater than the pivot with their exchange partner
            exchangeid = threadid + (tpg >> 1);
            local_arr_index = 0; local_arr_size = split;           
            exchange_arr[threadid] = local_arr + split; exchange_arr_sizes[threadid] = chunk_size - split;       
            pair_barrier_id = exchangeid;      
        } else {
            //Threads in the 'upper half' would exchange elements less than the pivot with their exchange partner
            exchangeid  = threadid - (tpg >> 1);
            local_arr_index = split; local_arr_size = chunk_size - split;
            exchange_arr[threadid] = local_arr;
            exchange_arr_sizes[threadid] = split;
            pair_barrier_id = threadid; //Here, we are always using the barrier of the upper half thread as convention
        }

        pthread_barrier_wait(bar_pair + pair_barrier_id);

        //Calculating the new chunk_size after exchange
        chunk_size = local_arr_size + exchange_arr_sizes[exchangeid];
        merged_arr = (int*) malloc(chunk_size * sizeof(int));
        
        int i = 0, j = 0, k = 0;
        int * local_index = local_arr + local_arr_index;
        int * exchange_index = exchange_arr[exchangeid];
        int exchange_arr_size = exchange_arr_sizes[exchangeid];

        while (j < local_arr_size && k < exchange_arr_size) {
            if (local_index[j] < exchange_index[k]) {
                merged_arr[i++] = local_index[j++];
            } else {
                merged_arr[i++] = exchange_index[k++];
            }
        }
        while (j < local_arr_size) {
            merged_arr[i++] = local_index[j++];
        }
        while (k < exchange_arr_size) {
            merged_arr[i++] = exchange_index[k++];
        }

        
        pthread_barrier_wait(bar_pair + pair_barrier_id);

        // iterate
        free(local_arr);
        local_arr = merged_arr;           
        thread_local_arr[threadid] = local_arr;      
          
        group_barrier_id += gpi; //Move on to using the next set of barriers for groups of threads
        tpg >>= 1; //Divide the number of threads/group by 2
        gpi <<= 1; //Multiply the number of groups/iterations by 2
    }

    //Final chunk_size of the local_arr after all the swaps
    local_sizes[threadid] = chunk_size;

    if (NT != 1) {
        //More than one thread executed
        pthread_barrier_wait(bar_group);
    }
        
    //Find the global location of the sorted local array = cumulative sizes of local arrays before current thread's local array
    int global_loc = 0;
    for (int i = 0; i < threadid; i++)
        global_loc += local_sizes[i];
    
    memcpy(arr + global_loc, local_arr, chunk_size * sizeof(int));

    // free thread local memory
    free(t_args);
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


double get_time() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}