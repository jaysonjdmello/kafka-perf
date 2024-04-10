#include <glib.h>
#include <librdkafka/rdkafka.h>

#include "common.c"

void basic_consume_loop(rd_kafka_t *rk) {
  rd_kafka_resp_err_t err;

  const char *topic = "data-plane-perf";

  // Create a list of topics to subscribe to
  rd_kafka_topic_partition_list_t *subscription;
  subscription = rd_kafka_topic_partition_list_new(
      1); // Assuming we'll subscribe to one topic for now
  rd_kafka_topic_partition_list_add(
      subscription, topic,
      RD_KAFKA_PARTITION_UA); // UA = Unassigned partition

  if ((err = rd_kafka_subscribe(rk, subscription))) {
    fprintf(stderr, "%% Failed to start consuming topics: %s\n",
            rd_kafka_err2str(err));
    exit(1);
  }

  while (1) {
    rd_kafka_message_t *rkmessage = rd_kafka_consumer_poll(rk, 500);
    if (rkmessage) {
      rd_kafka_message_destroy(rkmessage);
    }
  }

  err = rd_kafka_consumer_close(rk);
  if (err)
    fprintf(stderr, "%% Failed to close consumer: %s\n", rd_kafka_err2str(err));
  else
    fprintf(stderr, "%% Consumer closed\n");
}

int main(int argc, char **argv) {
  rd_kafka_t *consumer;
  rd_kafka_conf_t *conf;
  char errstr[512];

  // Parse the command line.
  if (argc != 2) {
    g_error("Usage: %s <config.ini>", argv[0]);
    return 1;
  }

  // Parse the configuration.
  // See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
  const char *config_file = argv[1];

  g_autoptr(GError) error = NULL;
  g_autoptr(GKeyFile) key_file = g_key_file_new();
  if (!g_key_file_load_from_file(key_file, config_file, G_KEY_FILE_NONE,
                                 &error)) {
    g_error("Error loading config file: %s", error->message);
    return 1;
  }

  // Load the relevant configuration sections.
  conf = rd_kafka_conf_new();
  load_config_group(conf, key_file, "default");

  // Create the Producer instance.
  consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
  if (!consumer) {
    g_error("Failed to create new producer: %s", errstr);
    return 1;
  }

  // Configuration object is now owned, and freed, by the rd_kafka_t instance.
  conf = NULL;

  basic_consume_loop(consumer);

  return 0;
}
