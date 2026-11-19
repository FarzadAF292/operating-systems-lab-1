
/*
 * Laboratory Work No. 1 — Monitors
 * Student: Farzad Afzalli
 * Group: 932404
 *
 * Producer-Consumer synchronization using POSIX threads.
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

#define EVENTS_COUNT 5

// Event structure
typedef struct {
    int id;
    char message[50];
} Event;

// Monitor structure
typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t condition;

    int ready;
    int stopping;
    void *data;
} Monitor;

// Initialize monitor
int monitor_init(Monitor *m) {
    if (pthread_mutex_init(&m->mutex, NULL) != 0) {
        return 0;
    }

    if (pthread_cond_init(&m->condition, NULL) != 0) {
        pthread_mutex_destroy(&m->mutex);
        return 0;
    }

    m->ready = 0;
    m->stopping = 0;
    m->data = NULL;

    return 1;
}

// Destroy monitor
void monitor_destroy(Monitor *m) {
    pthread_cond_destroy(&m->condition);
    pthread_mutex_destroy(&m->mutex);
}

// Send an event
int monitor_provide(Monitor *m, Event *event) {
    pthread_mutex_lock(&m->mutex);

    // Wait until the previous event is consumed
    while (m->ready && !m->stopping) {
        pthread_cond_wait(&m->condition, &m->mutex);
    }

    if (m->stopping) {
        pthread_mutex_unlock(&m->mutex);
        return 0;
    }

    m->data = event;
    m->ready = 1;

    printf("Producer: Event %d sent\n", event->id);

    // Notify consumer
    pthread_cond_signal(&m->condition);

    pthread_mutex_unlock(&m->mutex);
    return 1;
}

// Receive an event
Event *monitor_consume(Monitor *m) {
    pthread_mutex_lock(&m->mutex);

    // Wait until an event is available
    while (!m->ready && !m->stopping) {
        pthread_cond_wait(&m->condition, &m->mutex);
    }

    // Stop if there are no more events
    if (!m->ready) {
        pthread_mutex_unlock(&m->mutex);
        return NULL;
    }

    Event *event = (Event *)m->data;

    printf("Consumer: Event %d received (%s)\n",
           event->id, event->message);

    m->data = NULL;
    m->ready = 0;

    // Notify producer that the event was consumed
    pthread_cond_signal(&m->condition);

    pthread_mutex_unlock(&m->mutex);

    return event;
}

// Stop monitor
void monitor_stop(Monitor *m) {
    pthread_mutex_lock(&m->mutex);

    m->stopping = 1;

    // Wake up waiting threads
    pthread_cond_broadcast(&m->condition);

    pthread_mutex_unlock(&m->mutex);
}

// Producer thread
void *producer_thread(void *arg) {
    Monitor *m = (Monitor *)arg;

    for (int i = 1; i <= EVENTS_COUNT; i++) {

        // One-second delay
#ifdef _WIN32
        Sleep(1000);
#else
        sleep(1);
#endif

        // Create event
        Event *event = malloc(sizeof(Event));

        if (event == NULL) {
            fprintf(stderr, "Memory allocation failed\n");
            break;
        }

        event->id = i;

        snprintf(event->message, sizeof(event->message),
                 "Event number %d", i);

        // Send event to monitor
        if (!monitor_provide(m, event)) {
            free(event);
            break;
        }
    }

    monitor_stop(m);

    return NULL;
}

// Consumer thread
void *consumer_thread(void *arg) {
    Monitor *m = (Monitor *)arg;

    Event *event;

    while ((event = monitor_consume(m)) != NULL) {

        // Event processing is complete
        // Release dynamically allocated memory
        free(event);
    }

    return NULL;
}

// Main function
int main(void) {
    Monitor monitor;

    pthread_t producer;
    pthread_t consumer;

    // Initialize monitor
    if (!monitor_init(&monitor)) {
        fprintf(stderr, "Failed to initialize monitor\n");
        return EXIT_FAILURE;
    }

    // Create consumer thread
    if (pthread_create(&consumer, NULL,
                       consumer_thread, &monitor) != 0) {

        fprintf(stderr, "Failed to create consumer thread\n");
        monitor_destroy(&monitor);
        return EXIT_FAILURE;
    }

    // Create producer thread
    if (pthread_create(&producer, NULL,
                       producer_thread, &monitor) != 0) {

        fprintf(stderr, "Failed to create producer thread\n");

        monitor_stop(&monitor);
        pthread_join(consumer, NULL);
        monitor_destroy(&monitor);

        return EXIT_FAILURE;
    }

    // Wait for threads to finish
    pthread_join(producer, NULL);
    pthread_join(consumer, NULL);

    // Clean up synchronization resources
    monitor_destroy(&monitor);

    return EXIT_SUCCESS;
}
