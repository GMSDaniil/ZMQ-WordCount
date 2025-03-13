/*************************************************************
 *  zmq_distributor.c
 *  
 *  Distributor Logic:
 *    - Read the input file
 *    - Split it into chunks (ensuring words are not broken)
 *    - Send each "map" message (with a chunk) to worker processes
 *    - Collect responses from the map phase and parse them
 *    - Build and send "reduce" messages to a designated worker
 *    - Collect and merge the reduce results, sort them, then output
 *    - Finally, send a "rip" command to all workers to shut them down
 *************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>
#include <zmq.h>
#include <unistd.h>

#define MSG_SIZE 1500

// -------------------------------
// Structures
// -------------------------------
typedef struct Pair {
    char *word;
    int count;
    struct Pair *next;
} Pair;

static Pair *g_map_results = NULL;
static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;

typedef struct WorkerInfo {
    int index;
    char *endpoint;
    char *chunk;
} WorkerInfo;

void *g_zmq_context = NULL;

typedef struct FinalPair {
    char *word;
    int count;
    struct FinalPair *next;
} FinalPair;

static FinalPair *g_final_list = NULL;
static pthread_mutex_t g_final_lock = PTHREAD_MUTEX_INITIALIZER;

// -------------------------------
// Helper Functions
// -------------------------------

// Adds the (word, count) pair to the global map results linked list.
// If the word exists, its count is incremented; otherwise, a new node is appended.
static void add_to_global_map(const char *word, int c) {
    pthread_mutex_lock(&g_lock);

    Pair *p = g_map_results;

    while (p) {
        if (strcmp(p->word, word) == 0) {
            p->count += c;
            pthread_mutex_unlock(&g_lock);
            return;
        }
        p = p->next;
    }

    // Word not found; create a new Pair node.
    Pair *newp = malloc(sizeof(*newp));
    if (!newp) {
        pthread_mutex_unlock(&g_lock);
        return;
    }

    newp->word = strdup(word);
    if (!newp->word) {
        free(newp);
        pthread_mutex_unlock(&g_lock);
        return;
    }

    newp->count = c;
    newp->next = NULL;

    // Append new node to the end of the list.
    if (g_map_results == NULL) {
        g_map_results = newp;
    } else {
        Pair *last = g_map_results;
        while (last->next) {
            last = last->next;
        }
        last->next = newp;
    }

    pthread_mutex_unlock(&g_lock);
}


// Parses a map reply string from a worker.
// The reply format is: a word followed by several '1' characters (each '1' representing a count)
// e.g., "the11example111" represents "the" with count 2 and "example" with count 3.
static void parse_map_reply(const char *reply) {
    int i = 0;
    int n = (int)strlen(reply);

    while (i < n) {
        char word_buf[256];
        int wpos = 0;
        // Collect alphabetical characters to form a word.
        while (i < n && isalpha((unsigned char)reply[i])) {
            if (wpos < 255) {
                word_buf[wpos++] = reply[i];
            }
            i++;
        }
        word_buf[wpos] = '\0';

        int count = 0;
        // Count the contiguous '1's after the word.
        while (i < n && reply[i] == '1') {
            count++;
            i++;
        }

        // If both a word and its count were found, add to global map.
        if (wpos > 0 && count > 0) {
            add_to_global_map(word_buf, count);
        }
    }
}

// This function is run in a separate thread for each file chunk.
// It creates a ZeroMQ REQ socket, connects to the worker endpoint,
// sends a "map" request with the text chunk, receives the reply,
// and then parses the reply to update the global map.
static void *map_thread(void *arg) {
    WorkerInfo *info = (WorkerInfo *)arg;

    // Create a ZeroMQ REQ socket
    void *req = zmq_socket(g_zmq_context, ZMQ_REQ);
    while (!req) {
        sleep(0.05);
        req = zmq_socket(g_zmq_context, ZMQ_REQ);
    }

    // Set socket linger option to 0 (immediate close)
    int linger = 0;
    zmq_setsockopt(req, ZMQ_LINGER, &linger, sizeof(linger));

    // Connect to the specified worker endpoint
    if (zmq_connect(req, info->endpoint) != 0) {
        perror("zmq_connect map_thread");
        zmq_close(req);
        free(info->chunk);
        free(info);
        return NULL;
    }

    // Formulate the map message: prepend "map" to the text chunk
    char msg[MSG_SIZE];
    memset(msg, 0, sizeof(msg));
    snprintf(msg, sizeof(msg), "map%s", info->chunk);
    msg[MSG_SIZE - 1]= '\0';

    // Send the map message to the worker
    zmq_send(req, msg, strlen(msg)+1, 0);

    // Receive the reply from the worker
    char reply[MSG_SIZE];
    memset(reply, 0, sizeof(reply));
    int rsize = zmq_recv(req, reply, sizeof(reply) - 1, 0);
    if (rsize > 0) {
        reply[rsize] = '\0';
        parse_map_reply(reply);
    }

    // Clean up
    free(info->chunk);
    free(info);
    zmq_close(req);
    return NULL;
}

// -------------------------------
// Build Reduce Payload
// -------------------------------
// Constructs the reduce payload string which starts with "red"
// followed by each word and a series of '1's for each occurrence.
// As words are fully processed (count reaches 0), they are removed
// from the global map list.
static void build_reduce_payload(char *out, size_t outsize) {
    memset(out, 0, outsize);
    strcpy(out, "red"); // Payload starts with "red"
    size_t pos = 3;
    pthread_mutex_lock(&g_lock);
    Pair *prev = NULL; // To keep track of previous node for deletion
    Pair *p = g_map_results;

    while (p != NULL) {
        const char *w = p->word;
        int wlen = (int)strlen(w);
        // Append the word if there is enough space in the output buffer.
        if (pos + wlen < outsize - 1) {
            memcpy(out + pos, w, wlen);
            pos += wlen;
        } else {
            break; // Not enough space to add the word, so break out.
        }
        // Append '1' characters for each count of the word.
        while (p->count > 0 && pos < outsize - 1) {
            out[pos++] = '1';
            p->count--;
        }

        // If this word is fully processed, remove it from the list
        if (p->count == 0) {
            if (prev == NULL) {
                // Head of the list
                g_map_results = p->next;
                free(p->word);
                free(p);
                p = g_map_results;
            } else {
                // Middle or end of the list
                prev->next = p->next;
                free(p->word);
                free(p);
                p = prev->next;
            }
        } else {
            prev = p;
            p = p->next;
        }
    }
    pthread_mutex_unlock(&g_lock);
}


// Adds a (word, count) pair to the final results list (g_final_list).
// If the word exists, increments its count; otherwise, creates a new node.
static void add_to_final_list(const char *word, int c) {
    pthread_mutex_lock(&g_final_lock);
    FinalPair *fp = g_final_list;
    while (fp) {
        if (strcmp(fp->word, word) == 0) {
            fp->count += c;
            pthread_mutex_unlock(&g_final_lock);
            return;
        }
        fp = fp->next;
    }
    // Word not found; create a new FinalPair node.
    FinalPair *newf = malloc(sizeof(*newf));
    newf->word = strdup(word);
    newf->count = c;
    newf->next = g_final_list;
    g_final_list = newf;
    pthread_mutex_unlock(&g_final_lock);
}

// Parses the reply from a reduce worker. The expected format is a word
// followed by a numeric count (which can be multi-digit). Each parsed pair
// is added to the final list.
static void parse_reduce_reply(const char *reply) {
    int i = 0;
    int n = (int)strlen(reply);

    while (i < n) {
        char word_buf[256];
        int wpos = 0;
        // Collect the word (alphabetic characters).
        while (i < n && isalpha((unsigned char)reply[i])) {
            if (wpos < 255) {
                word_buf[wpos++] = reply[i];
            }
            i++;
        }
        word_buf[wpos] = '\0';

        char num_buf[64];
        int np = 0;
        // Collect the numeric count (digit characters).
        while (i < n && isdigit((unsigned char)reply[i])) {
            if (np < 63) {
                num_buf[np++] = reply[i];
            }
            i++;
        }
        num_buf[np] = '\0';

        // If both word and count were collected, add them to the final list.
        if (wpos > 0 && np > 0) {
            int c = atoi(num_buf);

            add_to_final_list(word_buf, c);
        }
    }
}

// Comparison function for sorting FinalPair nodes.
// It sorts by descending frequency, then by alphabetical order if frequencies are equal.
static int cmpfunc(const void *a, const void *b) {
    const FinalPair *fa = *(const FinalPair **)a;
    const FinalPair *fb = *(const FinalPair **)b;
    if (fa->count > fb->count) return -1;
    if (fa->count < fb->count) return +1;
    return strcmp(fa->word, fb->word);
}

// -----------------------------------------------------------------------------
// MAIN Distributor
// -----------------------------------------------------------------------------
int main(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <file.txt> <port1> [<port2> ...]\n", argv[0]);
        return 1;
    }


    int num_workers = argc - 2;
    char **endpoints = malloc(num_workers * sizeof(char*));
    // Build ZeroMQ endpoints from the port numbers provided.
    for (int i = 0; i < num_workers; i++) {
        char buf[64];
        snprintf(buf, sizeof(buf), "tcp://localhost:%s", argv[i+2]);
        endpoints[i] = strdup(buf);
    }

    g_zmq_context = zmq_ctx_new();
    if (!g_zmq_context) {
        fprintf(stderr, "zmq_ctx_new error\n");
        return 1;
    }

    // Open and read the input file completely into memory.
    const char *filename = argv[1];
    FILE *fp = fopen(filename, "r");
    if (!fp) {
        perror("fopen");
        return 1;
    }
    fseek(fp, 0, SEEK_END);
    long fsize = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    char *filecontent = malloc(fsize + 1);
    if (!filecontent) {
        fprintf(stderr, "Not enough memory\n");
        fclose(fp);
        return 1;
    }
    fread(filecontent, 1, fsize, fp);
    filecontent[fsize] = '\0';
    fclose(fp);

    // -------------------------------
    // Map Phase: Start threads to process each file chunk.
    // -------------------------------
    pthread_t threads[1024];
    int thread_count = 0;

    char *file_ptr = filecontent; // Pointer to track the current position in filecontent
    size_t max_chunk_size = 1496;
    while (*file_ptr) {
        WorkerInfo *wi = malloc(sizeof(*wi));

        // Choose a worker in round-robin fashion
        static int worker_index = 0;
        int widx = worker_index % num_workers;
        worker_index++;

        wi->index = widx;
        wi->endpoint = endpoints[widx];

        // Determine the actual chunk size (not breaking the file content too abruptly).
        size_t len = strlen(file_ptr);
        size_t actual_chunk_size = (len > max_chunk_size) ? max_chunk_size : len;

        // Ensure the chunk ends at a space if it's not the last part of the file
        if (actual_chunk_size < len && file_ptr[actual_chunk_size] != ' ') {
            while (actual_chunk_size > 0 && file_ptr[actual_chunk_size - 1] != ' ') {
                actual_chunk_size--;
            }
        }

        // Create a new chunk with the extracted part
        wi->chunk = strndup(file_ptr, actual_chunk_size);

        // Update the file pointer to the next part of the file
        file_ptr += actual_chunk_size;

        // Skip leading spaces to start at the next word
        while (*file_ptr == ' ') {
            file_ptr++;
        }

        // Create the thread to process this chunk
        pthread_create(&threads[thread_count], NULL, map_thread, wi);
        thread_count++;
    }

    // Wait for all map threads to finish.
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }

    // -------------------------------
    // Reduce Phase: Use worker[0] for reducing.
    // -------------------------------
    void *reduce_socket = zmq_socket(g_zmq_context, ZMQ_REQ);
    if (!reduce_socket) {
        perror("zmq_socket reduce");
        return 1;
    }

    int linger = 0;
    zmq_setsockopt(reduce_socket, ZMQ_LINGER, &linger, sizeof(linger));

    if (zmq_connect(reduce_socket, endpoints[0]) != 0) {
        perror("zmq_connect reduce");
        return 1;
    }

    char reduce_msg[MSG_SIZE];

    // Keep sending reduce messages until all map results are processed.
    while (1) {
        pthread_mutex_lock(&g_lock);
        if (g_map_results == NULL) {
            pthread_mutex_unlock(&g_lock);
            break; // Exit the loop when there are no more map results.
        }
        pthread_mutex_unlock(&g_lock);

        // Build the reduce payload
        memset(reduce_msg, 0, sizeof(reduce_msg));
        build_reduce_payload(reduce_msg, MSG_SIZE);
        reduce_msg[MSG_SIZE - 1] = '\0';

        // Send the reduce message to the worker
        if (zmq_send(reduce_socket, reduce_msg, strlen(reduce_msg)+1, 0) == -1) {
            perror("zmq_send reduce");
            break;
        }

        // Receive the reduce reply from the worker
        char reduce_reply[MSG_SIZE];
        memset(reduce_reply, 0, sizeof(reduce_reply));
        int r = zmq_recv(reduce_socket, reduce_reply, MSG_SIZE - 1, 0);
        if (r > 0) {
            reduce_reply[r] = '\0';
            parse_reduce_reply(reduce_reply);
        } else if (r == -1) {
            perror("zmq_recv reduce");
            break;
        }
    }


    zmq_close(reduce_socket);

    // KILL THEM ALLL
    for (int i = 0; i < num_workers; i++) {
        fprintf(stderr, "Sending rip");
        void *s = zmq_socket(g_zmq_context, ZMQ_REQ);
        if (!s) {
            perror("zmq_socket rip");
            zmq_close(s);
            zmq_ctx_destroy(g_zmq_context);
            return 1;
        }
        zmq_setsockopt(s, ZMQ_LINGER, &linger, sizeof(linger));
        if (zmq_connect(s, endpoints[i]) != 0) {
            perror("zmq_connect rip");
            zmq_close(s);
            zmq_ctx_destroy(g_zmq_context);
            return 1;
        }

        // Send the "rip" message to signal termination.
        zmq_send(s, "rip\0", 4, 0);

        char rbuf[MSG_SIZE];
        int rr2 = zmq_recv(s, rbuf, MSG_SIZE - 1, 0);
        if (rr2 > 0) {
            rbuf[rr2] = '\0';
            // We expect a "rip" response from the worker.
        }

        zmq_close(s);

    }

    zmq_ctx_destroy(g_zmq_context);

    // -------------------------------
    // Sort and Output Final Results
    // -------------------------------
    // Count how many unique words are in the final list.
    int count_final = 0;
    pthread_mutex_lock(&g_final_lock);
    {
        FinalPair *fp2 = g_final_list;
        while (fp2) {
            count_final++;
            fp2 = fp2->next;
        }
    }
    pthread_mutex_unlock(&g_final_lock);

    // Allocate an array to sort the final results.
    FinalPair **arr = malloc(count_final * sizeof(FinalPair*));
    pthread_mutex_lock(&g_final_lock);
    {
        FinalPair *fp2 = g_final_list;
        int idx = 0;
        while (fp2) {
            arr[idx++] = fp2;
            fp2 = fp2->next;
        }
    }
    pthread_mutex_unlock(&g_final_lock);

    // Sort the array by frequency (descending) and then alphabetically.
    qsort(arr, count_final, sizeof(FinalPair*), cmpfunc);
    fprintf(stderr, "Output of %d words", count_final);

    // Print the results in CSV format.
    printf("word,frequency\n");
    for (int i = 0; i < count_final; i++) {
        printf("%s,%d\n", arr[i]->word, arr[i]->count);

    }
    fprintf(stderr, "Printed csv");
    free(arr);

    // Free memory allocated for endpoints.
    for (int i = 0; i < num_workers; i++) {
        free(endpoints[i]);
    }
    free(endpoints);
    free(filecontent);

    return 0;
}
