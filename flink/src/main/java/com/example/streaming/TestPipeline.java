package com.example.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.json.JSONObject;
import java.sql.Types;
import java.util.Objects;

public class TestPipeline {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = createExecutionEnvironment();

        // Configure Kafka source
        String kafkaUser = System.getenv().getOrDefault("KAFKA_ADMIN_USER", "admin");
        String kafkaPass = System.getenv().getOrDefault("KAFKA_ADMIN_PASSWORD", "Q1w2e3r+");

        String bootstrapServers = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS",
                "kafka-1762355055:9092"
        );

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setProperty("security.protocol", "SASL_PLAINTEXT")
                .setProperty("sasl.mechanism", "PLAIN")
                .setProperty(
                        "sasl.jaas.config",
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + kafkaUser + "\" password=\"" + kafkaPass + "\";"
                )
                .setTopics("test")
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Create data stream from Kafka
        DataStream<String> stream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .name("Kafka Test Source")
                .uid("kafka-test-source");

        // Transform the JSON data with null and NaN handling
        DataStream<Test> eventStream = stream.flatMap(new FlatMapFunction<String, Test>() {
            @Override
            public void flatMap(String value, org.apache.flink.util.Collector<Test> out) throws Exception {
                if (value == null || value.trim().isEmpty()) {
                    System.err.println("Received empty or null message, skipping.");
                    return;
                }

                try {
                    JSONObject json = new JSONObject(value);

                    Test event = new Test();
                    // Use safe parsing methods that handle null and NaN
                    event.setA(safeParseInt(json, "a"));
                    event.setB(safeParseDouble(json, "b"));
                    event.setC(safeParseString(json, "c"));
                    event.setDTTM(safeParseString(json, "dttm"));

                    if (event.getA() == null && event.getB() == null && event.getC() == null && event.getDTTM() == null) {
                        System.err.println("Parsed event contains only null fields, skipping.");
                        return;
                    }

                    out.collect(event);
                } catch (Exception parseError) {
                    String truncatedValue = value.length() > 200 ? value.substring(0, 200) + "..." : value;
                    System.err.println("Failed to parse JSON message: " + parseError.getMessage());
                    System.err.println("Payload snippet: " + truncatedValue);
                }
            }
            
            // Helper method to safely parse integers
            private Integer safeParseInt(JSONObject json, String key) {
                if (!json.has(key) || json.isNull(key)) return null;
                
                try {
                    if (json.get(key) instanceof String) {
                        String strValue = json.getString(key);
                        if ("NaN".equalsIgnoreCase(strValue)) return null;
                        return Integer.parseInt(strValue);
                    }
                    return json.getInt(key);
                } catch (Exception e) {
                    return null; // Handle parsing errors
                }
            }
            
            // Helper method to safely parse doubles
            private Double safeParseDouble(JSONObject json, String key) {
                if (!json.has(key) || json.isNull(key)) return null;
                
                try {
                    if (json.get(key) instanceof String) {
                        String strValue = json.getString(key);
                        if ("NaN".equalsIgnoreCase(strValue)) return null;
                        return Double.parseDouble(strValue);
                    }
                    return json.getDouble(key);
                } catch (Exception e) {
                    return null; // Handle parsing errors
                }
            }
            
            // Helper method to safely parse strings
            private String safeParseString(JSONObject json, String key) {
                if (!json.has(key) || json.isNull(key)) return null;
                try {
                    return json.getString(key);
                } catch (Exception e) {
                    return null;
                }
            }
        }).name("Parse test payload")
          .uid("parse-test-messages")
          .filter(Objects::nonNull)
          .name("Filter invalid test messages")
          .uid("filter-test-messages");

        // Create a JDBC sink to StarRocks with null handling
        String jdbcUrl = System.getenv().getOrDefault("STARROCKS_JDBC_URL", "jdbc:mysql://kube-starrocks-fe-search:9030/iceberg.rzdm_test");
        String jdbcUser = System.getenv().getOrDefault("STARROCKS_USER", "root");
        String jdbcPassword = System.getenv().getOrDefault("STARROCKS_PASSWORD", "Q1w2e3r+");

        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchIntervalMs(200)
                .withBatchSize(1000)
                .withMaxRetries(3)
                .build();

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername(jdbcUser)
                .withPassword(jdbcPassword)
                .build();

        JdbcStatementBuilder<Test> statementBuilder = (statement, event) -> {
            setNullableInt(statement, 1, event.getA());
            setNullableDouble(statement, 2, event.getB());
            setNullableString(statement, 3, event.getC());
            setNullableString(statement, 4, event.getDTTM());
        };

        JdbcSink<Test> starRocksSink = JdbcSink.<Test>builder()
                .withQueryStatement(
                        "INSERT INTO iceberg.rzdm_test.test (a, b, c, dttm) VALUES (?, ?, ?, ?)",
                        statementBuilder)
                .withExecutionOptions(executionOptions)
                .buildAtLeastOnce(connectionOptions);

        // Add the StarRocks sink
        eventStream
                .sinkTo(starRocksSink)
                .name("StarRocks JDBC Sink")
                .uid("test-jdbc-sink")
                .setParallelism(1);

        // Execute the streaming pipeline
        env.execute("Streaming Data Pipeline");
    }

    private static StreamExecutionEnvironment createExecutionEnvironment() {
        Configuration config = new Configuration();
        String restAddress = System.getenv().getOrDefault("FLINK_REST_ADDRESS", "flink-1762357787-jobmanager");
        int restPort = Integer.parseInt(System.getenv().getOrDefault("FLINK_REST_PORT", "8081"));
        config.set(RestOptions.ADDRESS, restAddress);
        config.set(RestOptions.PORT, restPort);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(config, TestPipeline.class.getClassLoader());
        env.setParallelism(1);
        return env;
    }

    // Helper method to set nullable integer values
    private static void setNullableInt(java.sql.PreparedStatement statement, int index, Integer value) throws java.sql.SQLException {
        if (value == null) {
            statement.setNull(index, Types.INTEGER);
        } else {
            statement.setInt(index, value);
        }
    }

    // Helper method to set nullable double values
    private static void setNullableDouble(java.sql.PreparedStatement statement, int index, Double value) throws java.sql.SQLException {
        if (value == null) {
            statement.setNull(index, Types.DOUBLE);
        } else {
            statement.setDouble(index, value);
        }
    }

    // Helper method to set nullable string values
    private static void setNullableString(java.sql.PreparedStatement statement, int index, String value) throws java.sql.SQLException {
        if (value == null) {
            statement.setNull(index, Types.VARCHAR);
        } else {
            statement.setString(index, value);
        }
    }

    // Event POJO class with nullable fields
    public static class Test {
        private Integer A;
        private Double B;
        private String C;
        private String DTTM;
        // Getters and setters for nullable fields
        public Integer getA() { return A; }
        public void setA(Integer A) { this.A = A; }
        public Double getB() { return B; }
        public void setB(Double B) { this.B = B; }
        public String getC() { return C; }
        public void setC(String C) { this.C = C; }
        public String getDTTM() { return DTTM; }
        public void setDTTM(String DTTM) { this.DTTM = DTTM; }
    }
}