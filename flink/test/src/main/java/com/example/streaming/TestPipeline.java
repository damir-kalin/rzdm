package com.example.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;

import org.json.JSONObject;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class TestPipeline {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        Configuration config = new Configuration();
        config.set(RestOptions.ADDRESS, "flink-1762357787-jobmanager");
        config.set(RestOptions.PORT, 8081);

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
        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();

        Sink<Test> starRocksSink = new StarRocksJdbcSink(
                "jdbc:mysql://kube-starrocks-fe-mysql:9030/iceberg.rzdm_test",
                "com.mysql.cj.jdbc.Driver",
                "root",
                "Q1w2e3r+",
                executionOptions);

        // Add the StarRocks sink
        eventStream.sinkTo(starRocksSink).name("StarRocks Sink");

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

    private static class StarRocksJdbcSink implements Sink<Test> {
        private final String jdbcUrl;
        private final String driverClassName;
        private final String username;
        private final String password;
        private final JdbcExecutionOptions executionOptions;

        private static final String INSERT_SQL = "INSERT INTO iceberg.rzdm_test.test (a, b, c, dttm) VALUES (?, ?, ?, ?)";

        private StarRocksJdbcSink(
                String jdbcUrl,
                String driverClassName,
                String username,
                String password,
                JdbcExecutionOptions executionOptions) {
            this.jdbcUrl = jdbcUrl;
            this.driverClassName = driverClassName;
            this.username = username;
            this.password = password;
            this.executionOptions = executionOptions;
        }

        @Override
        public SinkWriter<Test> createWriter(WriterInitContext context) throws IOException {
            try {
                return new StarRocksJdbcWriter(
                        jdbcUrl,
                        driverClassName,
                        username,
                        password,
                        executionOptions);
            } catch (SQLException | ClassNotFoundException e) {
                throw new IOException("Failed to create JDBC writer", e);
            }
        }
    }

    private static class StarRocksJdbcWriter implements SinkWriter<Test> {
        private final Connection connection;
        private final PreparedStatement statement;
        private final int batchSize;
        private final long batchIntervalMs;
        private final int maxRetries;

        private int currentBatchCount = 0;
        private long lastFlushTime;

        private StarRocksJdbcWriter(
                String jdbcUrl,
                String driverClassName,
                String username,
                String password,
                JdbcExecutionOptions executionOptions) throws SQLException, ClassNotFoundException {
            Class.forName(driverClassName);
            this.connection = DriverManager.getConnection(jdbcUrl, username, password);
            this.connection.setAutoCommit(false);
            this.statement = connection.prepareStatement(StarRocksJdbcSink.INSERT_SQL);
            this.batchSize = executionOptions.getBatchSize();
            this.batchIntervalMs = executionOptions.getBatchIntervalMs();
            this.maxRetries = executionOptions.getMaxRetries();
            this.lastFlushTime = System.currentTimeMillis();
        }

        @Override
        public void write(Test value, SinkWriter.Context context) throws IOException, InterruptedException {
            try {
                setNullableInt(statement, 1, value.getA());
                setNullableDouble(statement, 2, value.getB());
                setNullableString(statement, 3, value.getC());
                setNullableString(statement, 4, value.getDTTM());
                statement.addBatch();
                currentBatchCount++;
            } catch (SQLException e) {
                throw new IOException("Failed to add record to batch", e);
            }

            long now = System.currentTimeMillis();
            if (currentBatchCount >= batchSize || (batchIntervalMs > 0 && (now - lastFlushTime) >= batchIntervalMs)) {
                flush(false);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            if (currentBatchCount == 0) {
                return;
            }

            int attempt = 0;
            while (true) {
                try {
                    statement.executeBatch();
                    connection.commit();
                    statement.clearBatch();
                    currentBatchCount = 0;
                    lastFlushTime = System.currentTimeMillis();
                    return;
                } catch (SQLException e) {
                    try {
                        connection.rollback();
                    } catch (SQLException rollbackEx) {
                        e.addSuppressed(rollbackEx);
                    }
                    attempt++;
                    if (attempt > maxRetries) {
                        throw new IOException("Failed to flush JDBC batch after " + maxRetries + " retries", e);
                    }
                    Thread.sleep(1000L * attempt);
                }
            }
        }

        @Override
        public void close() throws Exception {
            try {
                flush(true);
            } finally {
                try {
                    statement.close();
                } finally {
                    connection.close();
                }
            }
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