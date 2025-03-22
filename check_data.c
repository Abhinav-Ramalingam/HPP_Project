#include <stdio.h>
#include <stdlib.h>

int main(int argc, char *argv[]) {
    // Ensure the correct number of arguments
    if (argc != 3) {
        printf("Usage: %s <N> <filename>\n", argv[0]);
        return 1;
    }

    // Parse command-line arguments
    int N = atoi(argv[1]);
    char *inputfile = argv[2];

    // Phase 1: Read input from file
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
            fclose(input_fp);
            free(arr);
            return 1;
        }
    }
    fclose(input_fp);

    // Phase 2: Check if the numbers are in ascending order
    int deviations = 0;
    for (int i = 1; i < N; i++) {
        if (arr[i] < arr[i - 1]) {
            deviations++;
            printf("Deviation at index %d: %d -> %d\n", i - 1, arr[i - 1], arr[i]);
        }
    }

    // Print the total number of deviations
    printf("Total deviations: %d\n", deviations);

    // Clean up
    free(arr);
    return 0;
}
