package com.example.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;

import org.json.JSONObject;
import java.sql.Types;

public class TestPipeline {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        Configuration config = new Configuration();
        config.setString(RestOptions.ADDRESS, "flink-1762357787-jobmanager");
        config.setInteger(RestOptions.PORT, 8081);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

        // Configure Kafka source
        String kafkaUser = System.getenv().getOrDefault("KAFKA_ADMIN_USER", "admin");
        String kafkaPass = System.getenv().getOrDefault("KAFKA_ADMIN_PASSWORD", "Q1w2e3r+");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-1762355055-controller-0-external:29092,kafka-1762355055-controller-1-external:29092,kafka-1762355055-controller-0-external:29092")
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
        DataStream<String> stream = env.fromSource(source, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Transform the JSON data with null and NaN handling
        DataStream<Test> eventStream = stream.map(new MapFunction<String, Test>() {
            @Override
            public Test map(String value) throws Exception {
                JSONObject json = new JSONObject(value);
                
                Test event = new Test();
                // Use safe parsing methods that handle null and NaN
                event.setA(safeParseInt(json, "a"));
                event.setB(safeParseDouble(json, "b"));
                event.setC(safeParseString(json, "c"));
                event.setDTTM(safeParseString(json, "dttm"));
                
                return event;
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
        });

        // Create a JDBC sink to StarRocks with null handling
        SinkFunction<Test> starRocksSink = JdbcSink.sink(
                "INSERT INTO iceberg.rzdm_test.test (a, b, c, dttm) VALUES (?, ?, ?, ?)",
                (statement, event) -> {
                    // Set values with null handling
                    setNullableInt(statement, 1, event.getA());
                    setNullableDouble(statement, 2, event.getB());
                    setNullableString(statement, 3, event.getC());
                    setNullableString(statement, 4, event.getDTTM());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://kube-starrocks-fe-mysql:9030/iceberg.rzdm_test")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("Q1w2e3r+")
                        .build()
        );

        // Add the StarRocks sink
        eventStream.addSink(starRocksSink);

        // Execute the streaming pipeline
        env.execute("Streaming Data Pipeline");
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