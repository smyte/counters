cc_binary(
    name = "counters",
    srcs = [
        "CountersServer.cpp",
    ],
    deps = [
        ":counters_decrement_kafka_store_consumer",
        ":counters_handler",
        ":counters_increment_kafka_consumer",
        "//pipeline:redis_pipeline_bootstrap",
        "//platform/gcloud:gcs",
    ],
    copts = [
        "--std=c++14",
    ],
)

cc_library(
    name = "counters_handler",
    srcs = [
        "IncrbyMergeOperator.h",
        "ZeroValueCompactionFilter.h",
        "CountersHandler.cpp",
    ],
    hdrs = [
        "CountersHandler.h",
    ],
    deps = [
        "//external:boost",
        "//external:folly",
        "//external:rocksdb",
        "//pipeline:transactional_redis_handler",
    ],
    copts = [
        "-std=c++14",
    ],
)

cc_test(
    name = "counters_handler_test",
    srcs = [
        "CountersHandlerTest.cpp"
    ],
    size = "small",
    deps = [
        ":counters_handler",
        "//codec:redis_value",
        "//external:boost",
        "//external:gmock_main",
        "//external:gtest",
        "//stesting:test_helpers",
    ],
    copts = [
        "-std=c++14",
    ],
)

cc_library(
    name = "counters_increment_kafka_consumer",
    srcs = [
        "CounterRecord.hh",
        "CountersIncrementKafkaConsumer.cpp",
    ],
    hdrs = [
        "CountersIncrementKafkaConsumer.h",
    ],
    deps = [
        "//external:avro",
        "//external:boost",
        "//external:folly",
        "//external:glog",
        "//external:librdkafka",
        "//infra/kafka:consumer",
    ],
    copts = [
        "-std=c++11",
    ],
)

cc_library(
    name = "counters_decrement_kafka_store_consumer",
    srcs = [
        "CounterRecord.hh",
        "CountersDecrementKafkaStoreConsumer.cpp",
    ],
    hdrs = [
        "CountersDecrementKafkaStoreConsumer.h",
    ],
    deps = [
        "//external:avro",
        "//external:boost",
        "//external:folly",
        "//external:glog",
        "//infra/kafka/store:consumer",
        "//pipeline:kafka_consumer_config",
    ],
    copts = [
        "-std=c++11",
    ],
)
