#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <semaphore.h>
#include "mapreduce.h"


struct node {
    char* key;
    char* value;
    int taken;
    struct node* next;
};

struct partition{
    pthread_mutex_t lock;
    struct node* head;
    struct node* array; //sorted array
    struct keyIndex* keys;
    int size;
    int numKeys;
};

struct keyIndex{
    char* key;
    int startingIndex;
};

// struct mapStruct {
//     Mapper *map;
//     char **argv;
// };
Mapper globalMap;
Reducer redu;
char **fileNames;
int fileNums;
Partitioner partitionFunc;
int numParti;
volatile int fileCounter = 1;
struct partition* partitions;

pthread_mutex_t mutex;

int comparator(const void* p1, const void* p2) {
    struct node* a = (struct node*)p1;
    struct node* b = (struct node*)p2;
    return strcmp(a->key, b->key);
}

char* get_next(char* key, int partitionNum) {
    // get to partition
    struct node* curr = (partitions + partitionNum)->array;
    while (curr != NULL) {
        if (strcmp(curr->key, key) == 0 && curr->taken == 0) {
            // check value
            curr->taken = 1;
            return curr->value; 
        }
        else {
            curr = curr->next;
        }
    }
    return NULL;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

// adding values and putting into partitions
void MR_Emit(char *key, char *value) {
    unsigned long index = partitionFunc(key, numParti);
    //printf("%d\n", index);
    pthread_mutex_lock(&((partitions + index)->lock));
    if((partitions + index)->head == NULL){
        (partitions + index)->head = malloc(sizeof(struct node));
        (partitions + index)->head->key = key;
        (partitions + index)->head->value = value;
        (partitions + index)->head->taken = 0;
        (partitions + index)->size = 0;
        (partitions + index)->head->next = NULL;
    }
    else {
        struct node* curr = (partitions + index)->head;
        while(curr->next != NULL) {
            curr = curr->next;
        }

        curr->next = malloc(sizeof(struct node));
        curr->next->key = key;
        curr->next->value = value;
        curr->next->taken = 0;
        (partitions + index)->size++;
        curr->next->next = NULL;
    }
    pthread_mutex_unlock(&((partitions + index)->lock));
}

void wrapperMap(void* map) {
    //struct mapStruct mapStuff = (struct mapStruct)map;
    while(1) {
        char* fileName;
        pthread_mutex_lock(&mutex);
        if(fileCounter > fileNums){
            pthread_mutex_unlock(&mutex);
            break;
        }
        fileName = fileNames[fileCounter];
        fileCounter++;
        pthread_mutex_unlock(&mutex);
        globalMap(fileName); //FIXME
    }

}

void wrapperReducer(void* reduce) {

}

unsigned long MR_SortedPartition(char *key, int num_partitions) {

}
void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition, int num_partitions){
    partitions = malloc(sizeof(struct partition) * num_partitions);
    pthread_t *pthread = malloc(sizeof(pthread_t) * num_mappers);
    fileNames = argv;
    int threads = 0;
    globalMap = map;
    fileNums = argc-1;
    numParti = num_partitions;
    partitionFunc = partition;
    if(argc-1 < num_mappers) {
        threads = argc-1;
    }
    else {
        threads = num_mappers;
    }
    // struct mapStruct mapStuff;
    // mapStuff.map = map;
    // mapStuff.argv = argv;
    for (int i = 0; i < threads; ++i) { //create mapper threads
        pthread_create((pthread + i), NULL, (void*)wrapperMap, NULL);
    }

    for(int i = 0; i < threads; i++) { //join mapper threads
        pthread_join(*(pthread + i), NULL);
    }

    for(int x = 0; x < threads; x++) {
        free((pthread + x));
    }
    pthread = NULL;

    //turn every linkedList in each partition into an array and then sorts array, finds all keys in each partition and creates an array of key and starting indices
    for(int x = 0; x < num_partitions; x++) {
        (partitions + x)->array = malloc(sizeof(struct node) * (partitions + x)->size);
        struct node* curr = (partitions + x)->head;
        struct node* array = (partitions + x)->array;
        for(int y = 0; y < (partitions + x)->size; y++) {
            *(array + y) = *curr;
            curr = curr->next;
        }
        qsort((partitions + x)->array, (partitions + x)->size, sizeof(*((partitions + x)->array)), comparator);
        char* currKey = (partitions + x)->array->key;
        struct keyIndex a[300];
        a[0] = (struct keyIndex){.key = currKey, .startingIndex = 0};
        int keyCounter = 1;
        for(int z = 1; z < (partitions + x)->size; z++) {
            if(strcmp(currKey, ((partitions + x)->array+z)->key) != 0) {
                currKey = ((partitions + x)->array+z)->key;
                a[keyCounter] = (struct keyIndex){.key = currKey, .startingIndex = z};
                keyCounter++;
            }
        }
        (partitions + x)->numKeys = keyCounter;
        (partitions + x)->keys = malloc(sizeof(struct keyIndex*) * keyCounter);
        for(int i = 0; i < keyCounter; i++) {
            *((partitions + x)->keys + i) = a[i];
        }
    }

    pthread = malloc(sizeof(pthread_t) * num_reducers);

    

}



//create wrapper class
//make an infinite while loop where threads are competing for fileNames
//exit thread when all fileNames are taken