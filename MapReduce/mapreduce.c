#include "mapreduce.h"
#include "hashmap.h"
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct kv_
{
    char *key;
    char *value;
} kv;

typedef struct kv_array_
{
    kv **elements;
    size_t num_elements;
    size_t size;
    pthread_mutex_t lock;
    int cur_idx;
} kv_array;

// initialize objects
Mapper mapper;
int mappers_num;
Reducer reducer;
int reducers_num;
Partitioner partitioner;
// struct of partition - array of kv_array(array of kv)
kv_array **partitions_arr;
int num_partition;
// global variable storing arguments
char **global_argv = NULL;
// file lock
pthread_mutex_t filelock;
// current file
int current_file;
// file number
int file_number;
// store current thread index
int calledPartition;

// start of kv and kv_array data structure
void init_kv_array(kv_array *kvl, size_t size)
{
    kvl->elements = (kv **)malloc(size * sizeof(kv *));
    kvl->num_elements = 0;
    kvl->size = size;
    kvl->cur_idx=0;
    pthread_mutex_init(&kvl->lock, NULL);
}

void add_to_array(kv_array *kvl, kv *elt)
{
    pthread_mutex_lock(&kvl->lock);
    if (kvl->num_elements == kvl->size)
    {
        kvl->size *= 2;
        kvl->elements = realloc(kvl->elements, kvl->size * sizeof(kv *));
    }
    kvl->elements[kvl->num_elements++] = elt;
    pthread_mutex_unlock(&kvl->lock);
}

void init_global_argv(char *argv[], int argc)
{
    int file_number = argc - 1;
    global_argv = (char **)malloc(argc * sizeof(char *));
    for (int i = 0; i < file_number; i++)
    {
        global_argv[i] = strdup(argv[i + 1]);
    }
}

void free_global_argv(int argc)
{
    for (int i = 0; i < argc; i++)
    {
        free(global_argv[i]);
    }
    free(global_argv);
}

void free_partitions_arrs(int num_partitions)
{
    for (int i = 0; i < num_partitions; i++)
    {
        for (int j = 0; j < partitions_arr[i]->num_elements; j++)
        {
            free(partitions_arr[i]->elements[j]->key);
            free(partitions_arr[i]->elements[j]->value);
            free(partitions_arr[i]->elements[j]);
        }
        free(partitions_arr[i]->elements);
        free(partitions_arr[i]);
    }
    free(partitions_arr);
}

int cmp(const void *a, const void *b)
{
    char *str1 = (*(kv **)a)->key;
    char *str2 = (*(kv **)b)->key;
    return strcmp(str1, str2);
}

void MR_Emit(char *key, char *value)
{
    unsigned long partition_arr_position;
    if (partitioner != NULL)
    {
        partition_arr_position = partitioner(key, num_partition);
    }
    else
    {
        partition_arr_position = MR_DefaultHashPartition(key, num_partition);
    }
    kv *elt = (kv *)malloc(sizeof(kv));
    if (elt == NULL)
    {
        printf("Malloc error! %s\n", strerror(errno));
        exit(1);
    }
    elt->key = strdup(key);
    elt->value = strdup(value);
    add_to_array(partitions_arr[partition_arr_position], elt);
    return;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

void *find_fileAnd_map()
{
    while (1)
    {
        pthread_mutex_lock(&filelock);
        char *filename;
        if (current_file >= file_number)
        {
            pthread_mutex_unlock(&filelock);
            return NULL;
        }
        if (current_file <= file_number)
        {
            filename = global_argv[current_file];
            current_file++;
        }
        pthread_mutex_unlock(&filelock);
        mapper(filename);
    }
    return NULL;
}


char *get_next(char *key, int partition_number)
{
    if(partitions_arr[partition_number]->cur_idx >= partitions_arr[partition_number]->num_elements)
    {
        return NULL;
    }
    kv *current_ele = partitions_arr[partition_number]->elements[partitions_arr[partition_number]->cur_idx];
    if (!strcmp(current_ele->key, key))
    {
        pthread_mutex_lock(&partitions_arr[partition_number]->lock);
        partitions_arr[partition_number]->cur_idx++;
        pthread_mutex_unlock(&partitions_arr[partition_number]->lock);
        return current_ele->value;
    }
    return NULL;
}

void *ReduceHelper()
{
    pthread_mutex_lock(&filelock);
    calledPartition++;
    pthread_mutex_unlock(&filelock);
    int curr_idx = calledPartition - 1;
    while(partitions_arr[curr_idx]->cur_idx < partitions_arr[curr_idx]->num_elements)
    {
        reducer((partitions_arr[curr_idx]->elements[partitions_arr[curr_idx]->cur_idx])->key,
        get_next, curr_idx);
    }
    return NULL;
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers,
            Reducer reduce, int num_reducers, Partitioner partition)
{
    // check for valid input
    // No filename input
    if (argc == 0)
    {
        return;
    }

    // initialize global variable of argv
    init_global_argv(argv, argc);
    // search_word = strdup(argv[argc]);

    // initialize  variables
    mapper = map;
    mappers_num = num_mappers;
    reducer = reduce;
    reducers_num = num_reducers;
    partitioner = partition;
    // initialize mapper/reducer threads
    pthread_t mapperThreads[num_mappers];
    pthread_t reducerThreads[num_reducers];

    // malloc space for partitions's kv_array
    partitions_arr = (kv_array **)malloc(reducers_num * sizeof(kv_array *));
    num_partition = num_reducers;
    for (int i = 0; i < num_partition; i++)
    {
        kv_array *kvl = (kv_array *)malloc(sizeof(kv_array));
        init_kv_array(kvl, 100);
        partitions_arr[i] = kvl;
    }

    // Mapper process, each mapperThread try to call a file
    // Finding file process will need a file lock
    pthread_mutex_init(&filelock, NULL);
    current_file = 0;
    file_number = argc - 1;
    for (int i = 0; i < num_mappers; i++)
    {
        pthread_create(&mapperThreads[i], NULL, find_fileAnd_map, NULL);
    }

    // Start joining all the threads
    for (int i = 0; i < num_mappers; i++)
    {
        pthread_join(mapperThreads[i], NULL);
    }

    // using qsort
    for (int i = 0; i < num_partition; i++)
    {
        if (partitions_arr[i]->num_elements != 0)
        {
            qsort(partitions_arr[i]->elements, partitions_arr[i]->num_elements, sizeof(kv *), cmp);
        }
    }

    // initi calledPartition to keep track of called reducer
    calledPartition=0;
    for (int i = 0; i < reducers_num; i++)
    {
        pthread_create(&reducerThreads[i], NULL, ReduceHelper, NULL);
    }

    // Join the Reducers
    for (int i = 0; i < reducers_num; i++)
    {
        pthread_join(reducerThreads[i], NULL);
    }

    free_partitions_arrs(num_partition);
    free_global_argv(argc);

    return;
}
