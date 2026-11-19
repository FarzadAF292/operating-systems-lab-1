#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <windows.h>

// Event data
typedef struct {
    int id;
    char message[50];
} Event;

// Monitor
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t condition = PTHREAD_COND_INITIALIZER;

int ready = 0;
void* data = NULL;

// Producer thread
void* producer(void* arg) {
    for (int i = 1; i <= 5; i++) {
        Sleep(1000);

        // Create a new event
        Event* event = malloc(sizeof(Event));
        if (event == NULL) {
            fprintf(stderr, "Memory allocation failed\n");
            exit(EXIT_FAILURE);
        }

        event->id = i;
        snprintf(event->message, sizeof(event->message),
                 "Event number %d", i);

        pthread_mutex_lock(&mutex);

        // Wait until the previous event is consumed
        while (ready == 1) {
            pthread_cond_wait(&condition, &mutex);
        }

        data = event;
        ready = 1;

        printf("Producer: Event %d sent\n", event->id);

        pthread_cond_signal(&condition);
        pthread_mutex_unlock(&mutex);
    }

    return NULL;
}

// Consumer thread
void* consumer(void* arg) {
    for (int i = 1; i <= 5; i++) {
        pthread_mutex_lock(&mutex);

        // Wait for an event
        while (ready == 0) {
            pthread_cond_wait(&condition, &mutex);
        }

        // Receive the event pointer
        Event* event = (Event*)data;

        printf("Consumer: Event %d received (%s)\n",
               event->id, event->message);

        data = NULL;
        ready = 0;

        pthread_cond_signal(&condition);
        pthread_mutex_unlock(&mutex);

        // Free event memory after processing
        free(event);
    }

    return NULL;
}

int main(void) {
    pthread_t producer_thread;
    pthread_t consumer_thread;

    pthread_create(&producer_thread, NULL, producer, NULL);
    pthread_create(&consumer_thread, NULL, consumer, NULL);

    pthread_join(producer_thread, NULL);
    pthread_join(consumer_thread, NULL);

    pthread_cond_destroy(&condition);
    pthread_mutex_destroy(&mutex);

    return 0;
}
