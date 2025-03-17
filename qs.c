#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <omp.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

void quick_sort(int *, int, int);

int main(int ac, char* av[]){
    if(ac != 4){
        printf("Usage: ./%s N input_file output_file\n", av[0]);
        return 1;
    }
    int N = atoi(av[1]);
    char *input_file = av[2];
    char *output_file = av[3];

    double start = omp_get_wtime(), end;

    // Phase 1: Read input from file
    int *arr = (int *)calloc(N, sizeof(int));
    if (arr == NULL) {
        perror("Memory allocation failed");
        return 1;
    }

    int fd = open(input_file, O_RDONLY);
    if (fd == -1) {
        perror("Failed to open input file");
        free(arr);
        return 1;
    }

    char buffer[12]; 
    int index = 0;
    while (index < N && read(fd, buffer, sizeof(buffer) - 1) > 0) {
        arr[index++] = atoi(buffer);
    }
    close(fd);

    end = omp_get_wtime();
    printf("Input time(s): %lf\n", end - start);
    start = end;

    // Phase 2: Sorting the DS
    quick_sort(arr, 0, N - 1);

    end = omp_get_wtime();
    printf("Sorting time(s): %lf\n", end - start);
    start = end;

    // Phase 3: Write sorted numbers to output file
    fd = open(output_file, O_CREAT | O_WRONLY | O_TRUNC, 0666);
    if (fd == -1) {
        perror("Failed to open output file");
        free(arr);
        return 1;
    }

    for (int i = 0; i < N; i++) {
        char buffer[12];
        snprintf(buffer, sizeof(buffer), "%d ", arr[i]);
        write(fd, buffer, strlen(buffer));
    }
    write(fd, "\n", 1);
    close(fd);
    free(arr);

    end = omp_get_wtime();
    printf("Output time(s): %lf\n", end - start);

    return 0;
}

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
