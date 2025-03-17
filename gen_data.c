#include <stdio.h>
#include <stdlib.h>
#include <time.h>

void generate_random_numbers(int N, const char *filename) {
    srand(time(NULL));

    FILE *file = fopen(filename, "w");
    if (file == NULL) {
        perror("Failed to open file");
        exit(1);
    }

    for (int i = 0; i < N; i++) {
        fprintf(file, "%d ", rand() % 10000);
    }
    fprintf(file, "\n");

    fclose(file);
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <N>\n", argv[0]);
        return 1;
    }

    int N = atoi(argv[1]);
    generate_random_numbers(N, "numbers.txt");

    return 0;
}
