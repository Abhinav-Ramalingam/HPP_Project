#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <omp.h>
#include <string.h>

void quick_sort(int *, int, int);
void* thread_quick_sort(void *arg);
void* global_sort(void *arg);

// Structure to hold data for each thread
typedef struct {
    int *arr;
    int start;
    int end;
} local_data;

/*
typedef struct {
    int myid;
    int *arr;
    int *pivot;
} global_data;
*/

int main(int ac, char* av[]) {
    if (ac != 5) {
        printf("Usage: ./%s N input_file output_file N_Threads\n", av[0]);
        return 1;
    }
    int N = atoi(av[1]);
    char *input_file = av[2];
    char *output_file = av[3];
    int num_threads = atoi(av[4]);

    double start = omp_get_wtime(), end;

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

    end = omp_get_wtime();
    printf("Input time(s): %lf\n", end - start);
    start = end;

    // Phase 2: Sorting the data locally within threads
    pthread_t threads[num_threads];
    local_data data[num_threads];
    
    int chunk_size = N / num_threads;
    
    for (int t = 0; t < num_threads; t++) {
        data[t].arr = arr;
        data[t].start = t * chunk_size;
        data[t].end = (t == num_threads - 1) ? N - 1 : (t + 1) * chunk_size;
        pthread_create(&threads[t], NULL, thread_quick_sort, (void*)&data[t]);
    }

    for (int t = 0; t < num_threads; t++) {
        pthread_join(threads[t], NULL);
    }
/*
    //Phase 3: Sorting the data globally across threads
    for (int t = 0; t < num_threads; t++) {
        data[t].arr = arr;
        data[t].start = t * chunk_size;
        data[t].end = (t == num_threads - 1) ? N - 1 : (t + 1) * chunk_size;
        pthread_create(&threads[t], NULL, thread_quick_sort, (void*)&data[t]);
    }

    for (int t = 0; t < num_threads; t++) {
        pthread_join(threads[t], NULL);
    }

*/
    end = omp_get_wtime();
    printf("Sorting time(s): %lf\n", end - start);
    start = end;

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

    end = omp_get_wtime();
    printf("Output time(s): %lf\n", end - start);

    return 0;
}

// Function for thread to run quicksort on a portion of the array
void* thread_quick_sort(void *arg) {
    pthread_data *data = (pthread_data*)arg;
    quick_sort(data->arr, data->start, data->end);
    return NULL;
}

void*global_sort(void *arg){

}

// Standard quicksort function
void quick_sort(int *arr, int start, int end) {
    if (start >= end) return;

    int pivot = arr[end]; 
    int i = start - 1;

    for (int j = start; j < end; j++) {
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

    quick_sort(arr, start, i);
    quick_sort(arr, i + 2, end);
}
