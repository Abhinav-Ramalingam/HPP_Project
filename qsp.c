#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <omp.h>
#include <string.h>

void quick_sort(int *, int, int);
void* thread_quick_sort(void *arg);
void* global_sort(void *arg);

// Unified structure to hold data for each thread
typedef struct {
    int *arr;
    int begin;
    int end;
    int myid;
    int size;
    int chunk_size;
    int num_threads;
    int N;
    int *pivots;
} thread_data;

pthread_barrier_t barrier;

int main(int ac, char* av[]) {
    if (ac != 5) {
        printf("Usage: ./%s N input_file output_file N_Threads\n", av[0]);
        return 1;
    }
    int N = atoi(av[1]);
    char *input_file = av[2];
    char *output_file = av[3];
    int num_threads = atoi(av[4]);

    double start = omp_get_wtime(), stop;

    // Phase 1: Read input from file
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

    // Phase 2: Sorting the data locally within threads
    pthread_t threads[num_threads];
    thread_data tdata[num_threads];
    
    int chunk_size = N / num_threads;
    int pivots[num_threads];
    
    for (int t = 0; t < num_threads; t++) {
        tdata[t].arr = arr;
        tdata[t].begin = t * chunk_size;
        tdata[t].end = (t == num_threads - 1) ? N - 1 : (t + 1) * chunk_size - 1;
        printf("interval == %d!!\n",tdata[t].end - tdata[t].begin);
        tdata[t].pivots = pivots;
        tdata[t].myid = t;
        tdata[t].size = num_threads;
        tdata[t].num_threads = num_threads;
        tdata[t].chunk_size = chunk_size;
        tdata[t].N = N;
        pthread_create(&threads[t], NULL, thread_quick_sort, (void*)&tdata[t]);
    }

    for (int t = 0; t < num_threads; t++) {
        pthread_join(threads[t], NULL);
    }

    pthread_barrier_init(&barrier, NULL, num_threads);
    //Phase 3: Sorting the data globally across threads
    for (int t = 0; t < num_threads; t++) {
        pthread_create(&threads[t], NULL, global_sort, (void*)&tdata[t]);
    }
    
    for (int t = 0; t < num_threads; t++) {
        pthread_join(threads[t], NULL);
    }
    pthread_barrier_destroy(&barrier);

    stop = omp_get_wtime();
    printf("Sorting time(s): %lf\n", stop - start);
    start = stop;

    // Phase 4: Write sorted numbers to output file
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

// Function for thread to run quicksort on a portion of the array
void* thread_quick_sort(void *arg) {
    thread_data *data = (thread_data*)arg;
    quick_sort(data->arr, data->begin, data->end);
    return NULL;
}

void* global_sort(void *arg) {
    thread_data *tdata = (thread_data*)arg;
    int myid = tdata->myid;
    int size = tdata->size;
    int N = tdata->N;
    int *arr = tdata->arr;
    int chunk_size = tdata->chunk_size;
    int num_threads = tdata->num_threads;
    int *pivots = tdata->pivots;

    if(size == 1) return NULL;

    int localid = myid % size;
    int group = myid / size;

    int global_begin = myid * chunk_size;  
    int global_end = (myid == num_threads - 1) ? N - 1 : (myid + 1) * chunk_size - 1;
    pthread_barrier_wait(&barrier);
    if (localid == 0) {
        int pivot_index = (global_begin + global_end) / 2;
        pivots[group] = arr[pivot_index];
    }

    pthread_barrier_wait(&barrier);

    int pivot = pivots[group];

    int loop = global_begin;
    while(loop <= global_end && arr[loop] < pivot){
        loop++;
    }
    printf("Start = %d, End = %d\nThreadid is %d=%d:%d = pivot %d @ %d\n\n", global_begin, global_end, myid, group, localid, pivots[group], loop - global_begin);

    thread_data next_partition = {arr, global_begin, global_end, myid, size / 2, chunk_size, num_threads, N, pivots};
    global_sort(&next_partition);

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