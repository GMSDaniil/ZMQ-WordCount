/*************************************************************
 *  zmq_worker.c
 *  Worker logic:
 *   - Runs as ./zmq_worker <port1> [<port2> ...]
 *   - Creates a REP socket on each port
 *   - Handles "map...", "red...", and "rip" messages
 *   - Processes and responds to "map" / "red"
 *   - Responds "rip" to "rip" and exits
 *************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <zmq.h>
#include <unistd.h>

#define MSG_SIZE 1500

/**
 * Structure for the map/reduce operations.
 */
typedef struct WordCount {
    char *word;
    int count;
    struct WordCount *next;
} WordCount;

/**
 * Adds a word to the WordCount linked list.
 * If the word already exists, increments its count.
 * Otherwise, adds a new node at the end of the list.
 */
static void add_word(WordCount **head, const char *w, int c) {
    WordCount *cur = *head;

    // List is empty
    if (!cur) {
        WordCount *newwc = malloc(sizeof(*newwc));
        newwc->word = strdup(w); // Duplicate the word to avoid modifying input memory
        newwc->count = c;
        newwc->next = NULL;
        *head = newwc;
        return;
    }

    // Traverse the list to find the word or the last node
    while (cur) {
        // If the word already exists, increment the count
        if (strcmp(cur->word, w) == 0) {
            cur->count += c;
            return;
        }

        if (cur->next == NULL) {
            break;
        }

        cur = cur->next;
    }

    // If the word is not found, add a new node at the end
    WordCount *newwc = malloc(sizeof(*newwc));
    newwc->word = strdup(w);
    newwc->count = c;
    newwc->next = NULL;
    cur->next = newwc;
}



/**
 * Map function: Processes input text and extracts words.
 * Converts all non-alphabetic characters to spaces, converts to lowercase,
 * and counts occurrences of each word.
 *
 * Input:  "Hello11World22Hello"
 * Output: "hello11world1hello1"
 */
static char *map_function(const char *payload) {
    char *copy = strdup(payload);
   
    // Convert non-alphabetic characters to spaces and lowercase letters
    for (size_t i = 0; i < strlen(copy); i++) {
        if (!isalpha((unsigned char)copy[i])) {
            copy[i] = ' ';
        } else {
            copy[i] = (char)tolower((unsigned char)copy[i]);
        }
    }

    WordCount *head = NULL;
    char *token = strtok(copy, " \t\r\n");

    while (token) {
        add_word(&head, token, 1);
        token = strtok(NULL, " \t\r\n");
    }

    static char result[MSG_SIZE];
    memset(result, 0, sizeof(result));
    int pos = 0;

    // Build the response: "word + '1' * count"
    WordCount *p = head;
    while (p) {
        int wlen = (int)strlen(p->word);
        if (strcmp(p->word, "coenenchyma") == 0) {
                printf("Match found: %s\n", p->word);
                printf("%s\n", payload);
            }
        if (pos + wlen >= MSG_SIZE - 1) break;
        memcpy(result + pos, p->word, wlen);
        pos += wlen;

        // Append '1' for each occurrence
        for (int i = 0; i < p->count; i++) {
            if (pos >= MSG_SIZE - 1) break;
            result[pos++] = '1';
        }
        p = p->next;
    }


    // Free memory
    while (head) {
        WordCount *tmp = head;
        head = head->next;
        free(tmp->word);
        free(tmp);
    }
    free(copy);
    result[MSG_SIZE-1] = '\0';
    return result;
}

/**
 * Reduce function: Aggregates mapped results into final word counts.
 * Converts input from "word11word1" format to "word2word1".
 *
 * Input:  "hello111world111"
 * Output: "hello3world3"
 */
static char *reduce_function(const char *payload) {
    WordCount *head = NULL;

    int i = 0;
    int n = (int)strlen(payload);

    while (i < n) {
        // Extract the word
        char word_buf[256];
        int wpos = 0;
        while (i < n && isalpha((unsigned char)payload[i])) {
            if (wpos < 255) {
                word_buf[wpos++] = payload[i];
            }
            i++;
        }
        word_buf[wpos] = '\0';

        // Count consecutive '1's
        int count = 0;
        while (i < n && payload[i] == '1') {
            count++;
            i++;
        }

        // Store the word and count
        if (wpos > 0 && count > 0) {
            add_word(&head, word_buf, count);
        }
    }

    static char result[MSG_SIZE];
    memset(result, 0, sizeof(result));
    int pos = 0;

    // Build the final output: "word + count"
    WordCount *p = head;
    while (p) {
        int wlen = (int)strlen(p->word);
        if (pos + wlen >= MSG_SIZE - 1) break;
        memcpy(result + pos, p->word, wlen);
        pos += wlen;

        char numbuf[32];
        snprintf(numbuf, sizeof(numbuf), "%d", p->count);
        int numlen = (int)strlen(numbuf);
        if (pos + numlen >= MSG_SIZE - 1) break;
        memcpy(result + pos, numbuf, numlen);
        pos += numlen;

        p = p->next;
    }

    // Free memory
    while (head) {
        WordCount *tmp = head;
        head = head->next;
        free(tmp->word);
        free(tmp);
    }

    return result;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <port1> [<port2> ...]\n", argv[0]);
        return 1;
    }

    // Create ZMQ context
    void *context = zmq_ctx_new();
    if (!context) {
        perror("zmq_ctx_new");
        return 1;
    }

    // Create a REP socket and bind to specified ports
    void *responder = zmq_socket(context, ZMQ_REP);
    if (!responder) {
        perror("zmq_socket");
        zmq_ctx_destroy(context);
        return 1;
    }

    int linger = 0;
    zmq_setsockopt(responder, ZMQ_LINGER, &linger, sizeof(linger));

    int rcvtime = 2000;
    zmq_setsockopt(responder, ZMQ_RCVTIMEO, &rcvtime, sizeof(rcvtime));

    // Bind the worker to all specified ports
    for (int i = 1; i < argc; i++) {
        char endpoint[64];
        snprintf(endpoint, sizeof(endpoint), "tcp://*:%s", argv[i]);
        if (zmq_bind(responder, endpoint) != 0) {
            perror("zmq_bind");
        } else {
            printf("Worker bind to %s\n", endpoint);
            fflush(stdout);
        }
    }

    // Message handling loop
    while (1) {
        char buffer[MSG_SIZE];
        memset(buffer, 0, sizeof(buffer));
        int recv_size = zmq_recv(responder, buffer, sizeof(buffer) - 1, 0);
        if (recv_size < 0) {
            perror("zmq_recv");
            continue;
        }
        buffer[recv_size] = '\0';

        // Process message type and respond
        char type[4];
        memcpy(type, buffer, 3);
        type[3] = '\0';

        const char *payload = buffer + 3;

        char reply[MSG_SIZE];
        memset(reply, 0, sizeof(reply));

        if (strcmp(type, "map") == 0) {
            // map
            char *res = map_function(payload);
            strncpy(reply, res, MSG_SIZE - 1);
            reply[MSG_SIZE-1] = '\0';
            zmq_send(responder, reply, strlen(reply)+1, 0);
        }
        else if (strcmp(type, "red") == 0) {
            // reduce
            char *res = reduce_function(payload);
            strncpy(reply, res, MSG_SIZE - 1);
            reply[MSG_SIZE - 1] = '\0';
            zmq_send(responder, reply, strlen(reply)+1, 0);
        }
        else if (strcmp(type, "rip") == 0) {
            strcpy(reply, "rip");
            reply[MSG_SIZE - 1] = '\0';
            zmq_send(responder, reply, strlen(reply)+1, 0);
            printf("Worker received rip -> exiting\n");
            fflush(stdout);
            break;
        }
        else {
            zmq_send(responder, "", 0, 0);
        }

    }

    // Cleanup
    zmq_close(responder);
    zmq_ctx_destroy(context);
    printf("Worker done.\n");
    return 0;
}
