#include <glib.h>
#include <librdkafka/rdkafka.h>

#include "common.c"

#define THD_SIZE 64

#define KAFKA_PARTITION 0
#define TOPIC "wireguard-test-5"

static volatile sig_atomic_t run = 1;

g_autoptr(GKeyFile) key_file = NULL;

/**
 * @brief Signal termination of program
 */
static void stop(int sig) { run = 0; }

void *consumerThd(void *vargp) {
  rd_kafka_resp_err_t err;
  char errstr[512];

  rd_kafka_t *consumer;
  rd_kafka_conf_t *conf;

  // Load the relevant configuration sections.
  conf = rd_kafka_conf_new();
  load_config_group(conf, key_file, "default");
  load_config_group(conf, key_file, "consumer");

  // Create the Consumer instance.
  consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
  if (!consumer) {
    g_error("Failed to create new consumer: %s", errstr);
    return 1;
  }
  rd_kafka_poll_set_consumer(consumer);

  // Configuration object is now owned, and freed, by the rd_kafka_t instance.
  conf = NULL;

  // Convert the list of topics to a format suitable for librdkafka.
  const char *topic = TOPIC;
  rd_kafka_topic_partition_list_t *subscription =
      rd_kafka_topic_partition_list_new(1);
  rd_kafka_topic_partition_list_add(subscription, topic, KAFKA_PARTITION);

  // Subscribe to the list of topics.
  err = rd_kafka_subscribe(consumer, subscription);
  if (err) {
    g_error("Failed to subscribe to %d topics: %s", subscription->cnt,
            rd_kafka_err2str(err));
    rd_kafka_topic_partition_list_destroy(subscription);
    rd_kafka_destroy(consumer);
    return 1;
  }

  rd_kafka_topic_partition_list_destroy(subscription);

  // Install a signal handler for clean shutdown.
  signal(SIGINT, stop);

  // Start polling for messages.
  while (run) {
    rd_kafka_message_t *consumer_message;

    consumer_message = rd_kafka_consumer_poll(consumer, 500);
    if (!consumer_message) {
      g_message("Waiting...");
      continue;
    }

    if (consumer_message->err) {
      if (consumer_message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        /* We can ignore this error - it just means we've read
         * everything and are waiting for more data.
         */
      } else {
        g_message("Consumer error: %s",
                  rd_kafka_message_errstr(consumer_message));
        return;
      }
    } else {
#ifdef DEBUG
      g_message("Consumed event from topic %s: key = %.*s value = %s",
                rd_kafka_topic_name(consumer_message->rkt),
                (int)consumer_message->key_len, (char *)consumer_message->key,
                (char *)consumer_message->payload);
#endif
    }

    // Free the message when we're done.
    rd_kafka_message_destroy(consumer_message);
  }

  // Close the consumer: commit final offsets and leave the group.
  g_message("Closing consumer");
  rd_kafka_consumer_close(consumer);

  // Destroy the consumer.
  rd_kafka_destroy(consumer);
}

int main(int argc, char **argv) {
  // Parse the command line.
  if (argc != 2) {
    g_error("Usage: %s <config.ini>", argv[0]);
    return 1;
  }

  // Parse the configuration.
  // // See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
  const char *config_file = argv[1];

  g_autoptr(GError) error = NULL;
  key_file = g_key_file_new();
  if (!g_key_file_load_from_file(key_file, config_file, G_KEY_FILE_NONE,
                                 &error)) {
    g_error("Error loading config file: %s", error->message);
    return 1;
  }
  g_info("Starting %d Kafka Producer Threads", THD_SIZE);

  pthread_t thd[THD_SIZE];
  for (int i = 0; i < THD_SIZE; i++) {
    pthread_create(&thd[i], NULL, consumerThd, NULL);
  }

  for (int i = 0; i < THD_SIZE; i++) {
    pthread_join(thd[i], NULL);
  }

  return 0;
}
