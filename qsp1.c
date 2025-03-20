#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <omp.h>
#include <string.h>

typedef struct {
    int myid;
    int N;
    int num_threads;
    char strat;
    int *arr;
    int **thread_local_arr, **exchange_arr;
    int *pivots, *local_sizes, *exchange_sizes, *medians;
    pthread_barrier_t *barrier_groups_of_threads, *barrier_pairs_of_threads;
} thread_data;

void* thread_sort(void *);
void quick_sort(int *, int, int);

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
    char *input_file = av[2];
    char *output_file = av[3];
    int num_threads = atoi(av[4]);
    char strat = av[5][0];

    double start = omp_get_wtime(), stop;

    /**** PHASE 1: READ INPUT FROM FILE ****/

    int *arr = (int *)calloc(N, sizeof(int));
    if (arr == NULL) {
        perror("Memory allocation failed");
        return 1;
    }

    FILE *input_fp = fopen(input_file, "r");
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
    int num_threads_int_arr = sizeof(int*) * num_threads;
    int num_threads_int = sizeof(int) * num_threads;

    int ** thread_local_arr = (int **) (malloc(num_threads_int_arr));
    int * local_sizes = (int *) (malloc(num_threads_int));
    int ** exchange_arr = (int **) (malloc(num_threads_int_arr));
    int * exchange_sizes = (int *) (malloc(num_threads_int));
    int * pivots = (int *) (malloc(num_threads_int));
    int * medians = (int *) (malloc(num_threads_int));

    int t;
    pthread_barrier_t barrier_pairs_of_threads[num_threads];

    // Create barriers for synchronizing exchanging elements (1 barrier per processor)
    for (t = 0; t < num_threads; t++) {
        pthread_barrier_init(&barrier_pairs_of_threads[t], NULL, 2); // 2 - Pairs
    }

    // Create barriers for recursive levels: 1 barrier, 2 barriers, 4 barriers, etc.
    int barrier_count_groups = 0;
    for (t = 1; t < num_threads; t = t << 1) {
        barrier_count_groups += t;
    }
    pthread_barrier_t barrier_groups_of_threads[barrier_count_groups];
    int loop, bpg_index = 0;
    for (t = 1; t < num_threads; t = t << 1) {
        for (loop = 0; loop < t; loop++) {
            pthread_barrier_init(&barrier_groups_of_threads[bpg_index], NULL, num_threads / t);
            bpg_index++;
        }
    }

    pthread_t threads[num_threads];
    thread_data *tdata = (thread_data *)malloc(sizeof(thread_data) * num_threads);

    for (t = 0; t < num_threads; t++) {
        tdata[t].myid = t;
        tdata[t].N = N;
        tdata[t].num_threads = num_threads;
        tdata[t].strat = strat;
        tdata[t].arr = arr;
        tdata[t].thread_local_arr = thread_local_arr;
        tdata[t].exchange_arr = exchange_arr;
        tdata[t].pivots = pivots;
        tdata[t].local_sizes = local_sizes;
        tdata[t].exchange_sizes = exchange_sizes;
        tdata[t].medians = medians;
        tdata[t].barrier_groups_of_threads = barrier_groups_of_threads;
        tdata[t].barrier_pairs_of_threads = barrier_pairs_of_threads;

        pthread_create(&threads[t], NULL, thread_sort, (void *)&tdata[t]);
    }

    for (t = 0; t < num_threads; t++) {
        pthread_join(threads[t], NULL);
    }

    free(thread_local_arr); free(local_sizes);
    free(exchange_arr); free(exchange_sizes);
    free(pivots); free(medians);
    
    for(t = 0; t < num_threads; t++) {
        pthread_barrier_destroy(&barrier_pairs_of_threads[t]);
    }
    for(t = 0; t < barrier_count_groups; t++) {
        pthread_barrier_destroy(&barrier_groups_of_threads[t]);
    }

    stop = omp_get_wtime();
    printf("Sorting time(s): %lf\n", stop - start);
    start = stop;

    /**** PHASE 3: WRITE OUTPUT TO FILE ****/
    FILE *output_fp = fopen(output_file, "w");
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

void* thread_sort(void *args){
    thread_data* t_args =  (thread_data *) args;

    // Copying arguments to local variables
    int myid = t_args->myid;
    int N = t_args->N;
    int num_threads = t_args->num_threads;
    char strat = t_args->strat;
    int *arr = t_args->arr;
    int **thread_local_arr = t_args->thread_local_arr;
    int **exchange_arr = t_args->exchange_arr;
    int *pivots = t_args->pivots;
    int *local_sizes = t_args->local_sizes;
    int *exchange_sizes = t_args->exchange_sizes;
    int *medians = t_args->medians;
    pthread_barrier_t *barrier_groups_of_threads = t_args->barrier_groups_of_threads;
    pthread_barrier_t *barrier_pairs_of_threads = t_args->barrier_pairs_of_threads;

    int chunk_size = N / num_threads;
    int begin = myid * chunk_size;
    int end = (myid == num_threads - 1) ? N - 1 : (myid + 1) * chunk_size - 1;
    chunk_size = end - begin + 1; //Not all thread's actual chunk_size is N / num_threads

    //Create a local array of chunk_size in this thread executing the thread_sort
    int local_data = chunk_size * sizeof(int);
    int * local_arr = (int *) (malloc(local_data));
    thread_local_arr[myid] = local_arr;
    memcpy(local_arr, arr + begin, local_data);
    
    //local sort on LOCAL array
    quick_sort(local_arr, 0, chunk_size - 1);

    int tpg = num_threads;  //threads per group
    int gpi = 1;            // groups per iteration
    int localid, groupid;

    int group_barrier_offset = 0;
    int pair_barrier_offset = 0;

    while (tpg > 1){
        printf("\nTPG = %d\n", tpg);
        localid = myid % tpg;
        groupid = myid / tpg;

        // Median Calculation
        if (chunk_size > 0) {
            medians[myid] = local_arr[chunk_size / 2]; 
        }

        //Pivot Calculation
        pthread_barrier_wait(barrier_groups_of_threads + group_barrier_offset + groupid);
        if (localid == 0) {
            int pivot_index = (begin + end) / 2;
            pivots[groupid] = arr[pivot_index];
        }
        pthread_barrier_wait(barrier_groups_of_threads + group_barrier_offset + groupid);
        int pivot = pivots[groupid];

        //Splitpoint calculation
        int split = 0;
        while (split <= chunk_size && local_arr[split] < pivot) {
            split++;
        }
        printf("Start = %d, End = %d\nThreadid is myid=%d: %d:%d = pivot: %d @ %d: ", begin, end, myid, groupid, localid, pivot, split);

        tpg = tpg >> 1;
        group_barrier_offset += gpi;
        gpi = gpi << 1;
    }
    
    return NULL;

}

// Standard quicksort function
void quick_sort(int *arr, int begin, int end) {
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

    quick_sort(arr, begin, i);
    quick_sort(arr, i + 2, end);
}