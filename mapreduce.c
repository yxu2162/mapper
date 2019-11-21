#include <stddef.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "mapreduce.h"

struct node {
    char* key;
    char* value;
    struct node* next;
};

struct partition {
    pthread_mutex_t lock;
    long size;
    int headArray;
    struct node* head;
    struct node* array;
};

//Global
pthread_mutex_t mutex;
struct partition* partitions;
char** fileNames;
Mapper mapFunc;
Reducer reduceFunc;
Partitioner partitionFunc;
int numParti = 0;
int mapCounter = 1;
int numFiles = 0;
int partitionToRead = 0;
int bits = 0;

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

unsigned long MR_SortedPartition(char *key, int num_partitions) {
    unsigned long hash = atoi(key);
    hash = hash & 0x0FFFFFFFF;
    hash = hash >> (32 - bits);
    //printf("%ld\n", hash);
    return hash;
    // unsigned long hash = 5381;
    // int c;
    // while ((c = *key++) != '\0')
    //     hash = hash * 33 + c;
    // return hash % num_partitions;

}

void MR_Emit(char* key, char* value) {
    unsigned long index = partitionFunc(key, numParti);
    //printf("%ld\n", index);
    pthread_mutex_lock(&((partitions + index)->lock));
    if((partitions + index)->head == NULL) {
        (partitions + index)->head = malloc(sizeof(struct node));
        (partitions + index)->head->key = malloc(sizeof(key));
        strcpy((partitions + index)->head->key, key);
        (partitions + index)->head->value = value;
        (partitions + index)->size = 1;
        (partitions + index)->head->next = NULL;
    }
    else {
        struct node* curr = (partitions + index)->head;
        while(curr->next != NULL) {
            curr = curr->next;
        }
        curr->next = malloc(sizeof(struct node));
        curr->next->key = malloc(sizeof(key));
        strcpy(curr->next->key,key);
        curr->next->value = value;
        (partitions + index)->size++;
        curr->next->next = NULL;
    }
    pthread_mutex_unlock(&((partitions + index)->lock));
}

void wrapperMap(void* args) {
    while(1) {
        char* fileName;
        pthread_mutex_lock(&mutex);
        if(mapCounter > numFiles){
            pthread_mutex_unlock(&mutex);
            //printf("here\n");
            break;
        }
        fileName = *(fileNames + mapCounter);
        mapCounter++;
        pthread_mutex_unlock(&mutex);
        mapFunc(fileName); 
    }
}

char* get_next(char* key, int partitionNumber) {
    int index = partitionNumber;
    if((partitions + index)->headArray >= (partitions + index)->size){
        return NULL;
    }
    struct node* ele = ((partitions + index)->array + (partitions + index)->headArray);
    char* keyAtIndex = ele->key;
    if(strcmp(key, keyAtIndex) == 0) {
        char* toReturn = ele->value;
        (partitions + index)->headArray++;
        return toReturn;
    }
    return NULL;
}

void wrapperReducer(void* args) {
    struct partition* reader = NULL;
    char* keyToRead = NULL;
    int partitionNum = 0;
    while(1) {
        if(reader != NULL) {
            if(reader->headArray >= reader->size){
                pthread_mutex_lock(&mutex);
		while((partitions + partitionToRead)->size == 0 && partitionToRead < numParti) {
			partitionToRead++;
		}
		if(partitionToRead < numParti) {
		    partitionNum = partitionToRead;
		    reader = (partitions + partitionToRead);
		    partitionToRead++;
		    pthread_mutex_unlock(&mutex);
		}
		else{
		    pthread_mutex_unlock(&mutex);
		    break;
		}
            }
        }
        else {
            pthread_mutex_lock(&mutex);
            while((partitions + partitionToRead)->size == 0 && partitionToRead < numParti) {
		partitionToRead++;
            }
	    if(partitionToRead >= numParti) {
                pthread_mutex_unlock(&mutex);
                break;
            }
            partitionNum = partitionToRead;
            reader = (partitions + partitionToRead);
            partitionToRead++;
            //printf("%d\n", partitionToRead);
            pthread_mutex_unlock(&mutex);
        }
        char* keyToRead = (reader->array + reader->headArray)->key;
//	printf("%dkey\n", partitionToRead);
        reduceFunc(keyToRead, get_next, partitionNum);
    }
}
int comparator(const void* a, const void* b) {
    struct node* l = (struct node*)a;
    struct node* w = (struct node*)b;
    return strcmp(l->key, w->key);
}
void MR_Run(int argc, char *argv[], 
	    Mapper map, int num_mappers, 
	    Reducer reduce, int num_reducers, 
	    Partitioner partition, int num_partitions){

    partitions = malloc(sizeof(struct partition) * num_partitions);
    int threadSize = 0;
    fileNames = argv;
    mapFunc = map;
    reduceFunc = reduce;
    partitionFunc = partition;

    numParti = num_partitions;
    numFiles = argc-1;
    if(argc - 1 < num_mappers) {
        threadSize = argc - 1;
    }
    else {
        threadSize = num_mappers;
    }
    unsigned int num = num_partitions;
    while(num) {
        bits++;
        num >>= 1;
    }
    bits--;  

    pthread_t* threads = malloc(sizeof(pthread_t) * threadSize);
    for(int x = 0; x < threadSize; x++) {
        pthread_create((threads + x), NULL, (void*)wrapperMap, NULL);
    }
    for(int x = 0; x < threadSize; x++) {
        pthread_join(*(threads + x), NULL);
    }
    // for(int x = 0; x < threadSize; x++) {
    //     free(threads + x);
    // }



   // printf("%d\n", bits);

    for(int x = 0; x < num_partitions; x++) {
        //(partitions + x)->size--;
        long arraySize = (partitions + x)->size;
        (partitions + x)->array = malloc(sizeof(struct node) * arraySize);
        struct node* curr = (partitions + x)->head;
        struct node* array = (partitions + x)->array;
        for(int y = 0; y < arraySize; y++) {
            *(array + y) = *curr;
             curr = curr->next;
        }
        qsort((partitions + x)->array, arraySize, sizeof(struct node), comparator);
//	for(int y = 0; y < arraySize; y++) {
//		printf("%s\n", (array + y)->key);
//	}
    }

    if(num_partitions < num_reducers) {
        threadSize = num_partitions;
    }
    else {
        threadSize = num_reducers;
    }

    threads = realloc(threads, sizeof(pthread_t) * threadSize);

    for(int x = 0; x < threadSize; x++) {
        pthread_create((threads + x), NULL, (void*)wrapperReducer, NULL);
    }

    for(int x = 0; x < threadSize; x++) {
        pthread_join(*(threads + x), NULL);
    }


    
}
