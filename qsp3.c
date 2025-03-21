#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <sys/time.h>

// Standard quicksort function
static void local_sort(int* arr, const int lo, const int hi) {
    if (lo < hi) {
        const int mi = lo + ((hi - lo) >> 1);
        const int p = arr[mi];
        int tmp = arr[mi];
        arr[mi] = arr[hi];
        arr[hi] = tmp;
        
        int i = lo - 1;
        for (int j = lo; j < hi; j++) {
            if (arr[j] <= p) {
                i++;
                tmp = arr[i];
                arr[i] = arr[j];
                arr[j] = tmp;
            }
        }
        
        i++;
        tmp = arr[i];
        arr[i] = arr[hi];
        arr[hi] = tmp;
        
        local_sort(arr, lo, i - 1);
        local_sort(arr, i + 1, hi);
    }
}


typedef struct static_args_t {
    unsigned int   N;
    int NT;
    unsigned char  S;
    int **thread_local_arr, **exchange_arr;
    int *arr, *exchange_arr_sizes, *pivots, *local_sizes, *medians;
    pthread_barrier_t *bar_pair, *bar_group;
} static_args_t;

typedef struct args_t {
    int threadid;
    static_args_t* static_args;
} args_t;

static void* global_sort(void* targs) {

    // copy input
    const args_t*        args   = (args_t*) targs;
    const int threadid    = args->threadid;         // thread num
    const static_args_t* s_args = args->static_args;
    const unsigned int   N      = s_args->N;         // input size
    const int NT      = s_args->NT;         // num threads
    const unsigned char  S      = s_args->S;         // pivot strategy
    int*  arr   = s_args->arr;  // global input array
    int** thread_local_arr  = s_args->thread_local_arr; // local subarrays
    int** exchange_arr  = s_args->thread_local_arr; // local subarrays shifted to pivot index
    int*  exchange_arr_sizes  = s_args->exchange_arr_sizes; // size from/to pivot index in local subarrays
    int*  pivots   = s_args->pivots;  // pivot elements per local subarray
    int*  local_sizes   = s_args->local_sizes;  // size of local subarrays
    int*  medians   = s_args->medians;  // median elements per local subarray
    pthread_barrier_t* bar_pair = s_args->bar_pair; // pairs barriers
    pthread_barrier_t* bar_group = s_args->bar_group; // group barriers

    // inclusive lower bound of this thread's subarray on arr
    unsigned int lo = threadid * (N / NT);
    // inclusive upper bound of this thread's subarray on arr
    unsigned int hi = threadid < NT - 1 ? (threadid + 1) * (N / NT) - 1 : N - 1;
    // total number of elements in this thread's subarray on arr
    unsigned int n = hi - lo + 1;

    // copy to local subarray otherwise reallocating won't work
    int* ys  = (int*) malloc(n * sizeof(int));
    thread_local_arr[threadid] = ys;
    memcpy(ys, arr + lo, n * sizeof(int));
    int* merged_arr;
    medians[threadid] = 0;

    // sort subarray locally
    local_sort(ys, 0, n - 1);

    int lid, gid, rid; // local id, group id, partner thread id
    int t = NT;         // threads per group
    int p;                        // value of pivot element
    unsigned int local_arr_size;              // number of elements of subarray split according to p
    unsigned int local_arr_index;          // index shift of local subarray to reach split

    // for finding the correct barriers in the collective arrays
    int group_barrier_id = 0;
    int pair_barrier_id = 0;
    int gpi         = 1;

    // divide threads into groups until no smaller groups can be formed
    while (t > 1) {

        lid = threadid % t;
        gid = threadid / t;

        // update current median if elements exist
        // what is a good solution for n == 0?
        if (n > 0)
            medians[threadid] = ys[n >> 1];

        // wait for threads to finish splitting in previous iteration
        // otherwise they might split by the pivot element of the next iteration
        // also their median might not be updated yet
        pthread_barrier_wait(bar_group + group_barrier_id + gid);
        if (lid == 0) {
            if (S == 1) {
                // strategy 1
                // median of thread 0 of each group
                p = medians[threadid];
            } else if (S == 2) {
                // strategy 2
                // mean of all medians in a group
                long int mm = 0;
                for (int i = threadid; i < threadid + t; i++)
                    mm += medians[i];
                p = mm / t;
            } else if (S == 3) {
                // strategy 3
                // mean of center 2 medians in a group
                local_sort(medians, threadid, threadid + t - 1);
                p = (medians[threadid + (t >> 1) - 1] + medians[threadid + (t >> 1)]) >> 1;
            } else {
                // default to strategy 1
                p = medians[threadid];
            }
            // distribute pivot element within group
            for (int i = threadid; i < threadid + t; i++)
                    pivots[i] = p;
        }
        pthread_barrier_wait(bar_group + group_barrier_id + gid);

        // find split point according to pivot element
        p = pivots[threadid];
        int s = 0;
        while (s < n && ys[s] <= p) {
            s++;
        }

        pthread_barrier_wait(bar_group + group_barrier_id + gid);


        // send data
        if (lid < (t >> 1)) {
            // send upper part
            rid      = threadid + (t >> 1); // remote partner id
            local_arr_index   = 0;              // local shift to fit split point
            local_arr_size       = s;              // local size of lower part
            exchange_arr[threadid] = ys + s;         // remote shift to fit split point
            exchange_arr_sizes[threadid] = n - s;          // remote size of parnter's lower part
            pair_barrier_id = rid;      // shift to find partner's barrier
        } else {
            // send lower part
            rid      = threadid - (t >> 1);
            local_arr_index   = s;
            local_arr_size       = n - s;
            exchange_arr[threadid] = ys;
            exchange_arr_sizes[threadid] = s;
            pair_barrier_id = threadid;
        }
        // use barrier of the upper partner
        pthread_barrier_wait(bar_pair + pair_barrier_id);

        // merge local and remote elements in place of new local subarray
        n = local_arr_size + exchange_arr_sizes[rid];
        merged_arr = (int*) malloc(n * sizeof(int));
        
        int i = 0, j = 0, k = 0;
        while (j < local_arr_size && k < exchange_arr_sizes[rid]) {
            if ((ys + local_arr_index)[j] < exchange_arr[rid][k]) {
                merged_arr[i++] = (ys + local_arr_index)[j++];
            } else {
                merged_arr[i++] = exchange_arr[rid][k++];
            }
        }
        while (j < local_arr_size) {
            merged_arr[i++] = (ys + local_arr_index)[j++];
        }
        while (k < exchange_arr_sizes[rid]) {
            merged_arr[i++] = exchange_arr[rid][k++];
        }

        
        pthread_barrier_wait(bar_pair + pair_barrier_id);

        // iterate
        free(ys);
        ys              = merged_arr;          // swap local pointer
        thread_local_arr[threadid]        = ys;          // update global pointer
        t               = t >> 1;      // half group size
        group_barrier_id += gpi;      // shift group barrier pointer
        gpi          = gpi << 1; // double number of groups
    }

    // merge local subarrays back into arr
    // reservate space on arr
    local_sizes[threadid] = n;
    if (NT > 1) // else bar_group was never allocated
        pthread_barrier_wait(bar_group);
    // find location of local subarray on arr
    unsigned int n_prev = 0;
    for (int i = 0; i < threadid; i++)
        n_prev += local_sizes[i];
    // copy
    memcpy(arr + n_prev, ys, n * sizeof(int));

    // free thread local memory
    free(targs);
    free(ys);
    return NULL;
}


int main(int ac, char** av) {

    // parse input
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


    /**** PHASE 2: SORT INPUT ****/
    int** thread_local_arr = (int**) malloc(NT * sizeof(int*)); // local subarrays
    int** exchange_arr = (int**) malloc(NT * sizeof(int*)); // remote subarrays
    int*  exchange_arr_sizes = (int*)  malloc(NT * sizeof(int )); // number of elements 'sent'
    int*  pivots  = (int*)  malloc(NT * sizeof(int )); // pivot element for each thread
    int*  local_sizes  = (int*)  malloc(NT * sizeof(int )); // number of elements in each thread in arr
    int*  medians  = (int*)  malloc(NT * sizeof(int )); // median of each subarray
    
    pthread_barrier_t* bar_pair = (pthread_barrier_t*) malloc(NT * sizeof(pthread_barrier_t));
    for (int i = 0; i < NT; i++)
        pthread_barrier_init(bar_pair + i, NULL, 2);
    
    // let T = 16, then
    // bar_group = [ 0, 0, 1, 0, 1, 2, 3, 0, 1, 2, 3, 4, 5, 6, 7]
    // b_counts = [16, 8, 8, 4, 4, 4, 4, 2, 2, 2, 2, 2, 2, 2, 2]
    // num_bs_g =  1  +  2  +     4     +          8
    int bar_group_count = 0;
    for (int j = 1; j < NT; j = j << 1)
        bar_group_count += j;  
    pthread_barrier_t* bar_group = (pthread_barrier_t*) malloc(bar_group_count * sizeof(pthread_barrier_t));
    int l = 0;
    for (int j = 1; j < NT; j = j << 1)
        for (int k = 0; k < j; k++)
            pthread_barrier_init(bar_group + l++, NULL, NT / j);
    
    static_args_t static_args = {N, NT, strat, thread_local_arr, exchange_arr, arr, exchange_arr_sizes, pivots, local_sizes, medians, bar_pair, bar_group};
    pthread_t* threads = (pthread_t*) malloc(NT * sizeof(pthread_t));
    
    // fork
    for (int i = 0; i < NT; i++) {
        // malloc because otherwise it will reuse pointers or something
        args_t* targs = (args_t*) malloc(sizeof(args_t));
        targs->threadid = i;
        targs->static_args = &static_args;
        pthread_create(threads + i, NULL, global_sort, (void*) targs);
    }
    
    // join
    for (int i = 0; i < NT; i++)
        pthread_join(threads[i], NULL);
    
    // free shared memory
    free(thread_local_arr);
    free(exchange_arr);
    free(exchange_arr_sizes);
    free(pivots);
    free(local_sizes);
    free(medians);
    free(threads);
    for (int i = 0; i < NT; i++)
        pthread_barrier_destroy(bar_pair + i);
    for (int i = 0; i < bar_group_count; i++)
        pthread_barrier_destroy(bar_group + i);
    free(bar_pair);
    free(bar_group);
    

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
    return 0;
}