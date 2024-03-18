#include "common.c"
#include <glib.h>
#include <librdkafka/rdkafka.h>
#include <pthread.h>
#include <unistd.h>

#define ARR_SIZE(arr) (sizeof((arr)) / sizeof((arr[0])))

#define THD_SIZE 75

#define SEND_MESSAGE 1
#define MSG_SIZE 2

#define CONNECTION_ITERATIONS 100000
#define MESSAGE_COUNT_PER_ITERATION 100000
#define KAFKA_PARTITION 7

//#define SHOULD_POLL 1
//#define SHOULD_FLUSH 1

g_autoptr(GKeyFile) key_file = NULL;

void *producerThd(void *vargp) {
  char errstr[512];
  const char *topic = "data-plane-perf";
  const char *user_ids[6] = {"eabara",   "jsmith",  "sgarcia",
                             "jbernard", "htanaka", "awalther"};
  char product[MSG_SIZE];
  for (int k = 0; k < MSG_SIZE; k++) {
    product[k] = 'a';
  }

  for (int itr = 0; itr < CONNECTION_ITERATIONS; itr++) {

    // Load the relevant configuration sections.
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    load_config_group(conf, key_file, "default");

    // Create the Producer instance.
    rd_kafka_t *producer =
        rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!producer) {
      g_error("Failed to create new producer: %s", errstr);
      return NULL;
    }

    // Configuration object is now owned, and freed, by the rd_kafka_t instance.
    conf = NULL;

    g_info("created new producer connection");

#ifdef SEND_MESSAGE
    // Produce data by selecting random values from these lists.
    for (int i = 0; i < MESSAGE_COUNT_PER_ITERATION; i++) {
      const char *key = user_ids[random() % ARR_SIZE(user_ids)];
      const char *value = product;
      size_t key_len = strlen(key);
      size_t value_len = MSG_SIZE;

      rd_kafka_resp_err_t err;

      err = rd_kafka_producev(producer, RD_KAFKA_V_TOPIC(topic),
                              RD_KAFKA_V_PARTITION(KAFKA_PARTITION),
                              RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                              RD_KAFKA_V_KEY((void *)key, key_len),
                              RD_KAFKA_V_VALUE((void *)value, value_len),
                              RD_KAFKA_V_OPAQUE(NULL), RD_KAFKA_V_END);

      if (err) {
        g_error("Failed to produce to topic %s: %s", topic,
                rd_kafka_err2str(err));
        return NULL;
      }

#ifdef SHOULD_POLL
      rd_kafka_poll(producer, 0);
#endif
    }
#endif

#ifdef SHOULD_FLUSH
    rd_kafka_flush(producer, 60);
#endif
    rd_kafka_destroy(producer);
  }
  return NULL;
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
    pthread_create(&thd[i], NULL, producerThd, NULL);
  }

  for (int i = 0; i < THD_SIZE; i++) {
    pthread_join(thd[i], NULL);
  }

  return 0;
}
