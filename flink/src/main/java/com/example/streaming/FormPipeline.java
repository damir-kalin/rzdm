package com.example.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
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
import org.json.JSONArray;
import java.sql.Types;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Objects;

public class FormPipeline {

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
                .setTopics("sys__buinu__esud_rzdm__form__data")
                .setGroupId("flink-form-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Create data stream from Kafka
        DataStream<String> stream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Form Source")
                .name("Kafka Form Source")
                .uid("kafka-form-source");

        // Transform the JSON data - flatten forms and data records
        DataStream<Form> eventStream = stream.flatMap(new FlatMapFunction<String, Form>() {
            @Override
            public void flatMap(String value, Collector<Form> out) throws Exception {
                if (value == null || value.trim().isEmpty()) {
                    System.err.println("Warning: Received null or empty message, skipping...");
                    return;
                }

                String trimmedValue = value.trim();
                JSONObject formJson = null;

                try {
                    if (trimmedValue.startsWith("[")) {
                        JSONArray jsonArray = new JSONArray(trimmedValue);
                        if (jsonArray.length() > 0) {
                            formJson = jsonArray.getJSONObject(0);
                        } else {
                            System.err.println("Warning: JSON array is empty");
                            return;
                        }
                    } else if (trimmedValue.startsWith("{")) {
                        formJson = new JSONObject(trimmedValue);
                    } else {
                        System.err.println("Warning: Invalid JSON format: " +
                            (trimmedValue.length() > 100 ? trimmedValue.substring(0, 100) + "..." : trimmedValue));
                        return;
                    }
                } catch (org.json.JSONException e) {
                    System.err.println("Error parsing JSON: " + e.getMessage());
                    System.err.println("Problematic message (first 200 chars): " +
                        (trimmedValue.length() > 200 ? trimmedValue.substring(0, 200) + "..." : trimmedValue));
                    return;
                }

                // Extract form level fields
                String orgName = safeParseString(formJson, "orgname");
                String orgINN = safeParseString(formJson, "orginn");
                String orgKPP = safeParseString(formJson, "orgkpp");
                String kodForm = safeParseString(formJson, "kodform");
                String period1 = safeParseString(formJson, "period1");
                String period2 = safeParseString(formJson, "period2");

                // Process datarecord array
                JSONArray datarecordArray = formJson.optJSONArray("datarecord");
                if (datarecordArray != null) {
                    for (int i = 0; i < datarecordArray.length(); i++) {
                        JSONObject recordJson = datarecordArray.getJSONObject(i);
                        Form form = new Form();

                        form.setOrgName(orgName);
                        form.setOrgINN(orgINN);
                        form.setOrgKPP(orgKPP);
                        form.setKodForm(kodForm);
                        form.setPeriod1(parseDate(period1));
                        form.setPeriod2(parseDate(period2));
                        form.setKodstr(safeParseString(recordJson, "kodstr"));
                        form.setKodkol(parseInteger(safeParseString(recordJson, "kodkol")));
                        Object summaObj = recordJson.opt("summa");
                        form.setSumma(parseDouble(summaObj));
                        form.setLoadTimestamp(Timestamp.valueOf(LocalDateTime.now()));

                        out.collect(form);
                    }
                }
            }

            private String safeParseString(JSONObject json, String key) {
                if (json == null || !json.has(key) || json.isNull(key)) return null;
                try {
                    Object value = json.get(key);
                    if (value == null) return null;
                    return value.toString();
                } catch (Exception e) {
                    return null;
                }
            }

            private Integer parseInteger(String value) {
                if (value == null || value.trim().isEmpty()) return null;
                try {
                    return Integer.parseInt(value.trim());
                } catch (Exception e) {
                    return null;
                }
            }

            private Double parseDouble(Object valueObj) {
                if (valueObj == null) return null;
                try {
                    // Handle numeric values from JSON
                    if (valueObj instanceof Number) {
                        return ((Number) valueObj).doubleValue();
                    }
                    // Handle string values
                    String value = valueObj.toString();
                    if (value.trim().isEmpty()) return null;
                    // Remove spaces and replace comma with dot
                    String cleaned = value.replaceAll("[\\s,]", "").replace(",", ".");
                    return Double.parseDouble(cleaned);
                } catch (Exception e) {
                    return null;
                }
            }

            private Timestamp parseDate(String value) {
                if (value == null || value.trim().isEmpty()) return null;
                try {
                    // Format: "01.01.2025"
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy");
                    LocalDateTime dateTime = LocalDateTime.parse(value, formatter);
                    return Timestamp.valueOf(dateTime);
                } catch (Exception e) {
                    return null;
                }
            }
        }).name("Parse form payload")
          .uid("parse-forms");

        // Create JDBC sink to StarRocks
        String jdbcUrl = System.getenv().getOrDefault("STARROCKS_JDBC_URL", "jdbc:mysql://kube-starrocks-fe-service:9030/iceberg.rzdm_test");
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

        final int parameterCount = 10;

        String insertSql = "INSERT INTO iceberg.rzdm_test.stg_forms (" +
                    "orgname, orginn, orgkpp, kodform, period1, period2, " +
                    "kodstr, kodkol, summa, load_timestamp" +
                ") VALUES (" +
                    String.join(", ", Collections.nCopies(parameterCount, "?")) +
                ")";

        JdbcStatementBuilder<Form> statementBuilder = (statement, form) -> {
            int index = 1;
            setNullableString(statement, index++, form.getOrgName());
            setNullableString(statement, index++, form.getOrgINN());
            setNullableString(statement, index++, form.getOrgKPP());
            setNullableString(statement, index++, form.getKodForm());
            setNullableTimestamp(statement, index++, form.getPeriod1());
            setNullableTimestamp(statement, index++, form.getPeriod2());
            setNullableString(statement, index++, form.getKodstr());
            setNullableInteger(statement, index++, form.getKodkol());
            setNullableDouble(statement, index++, form.getSumma());
            setNullableTimestamp(statement, index++, form.getLoadTimestamp());
        };

        DataStream<Form> filteredStream = eventStream
                .filter(Objects::nonNull)
                .name("Filter null forms")
                .uid("filter-forms");

        JdbcSink<Form> starRocksSink = JdbcSink.<Form>builder()
                .withQueryStatement(insertSql, statementBuilder)
                .withExecutionOptions(executionOptions)
                .buildAtLeastOnce(connectionOptions);

        filteredStream
                .sinkTo(starRocksSink)
                .name("StarRocks JDBC Sink")
                .uid("form-jdbc-sink")
                .setParallelism(1);

        env.execute("Form Streaming Pipeline");
    }

    private static StreamExecutionEnvironment createExecutionEnvironment() {
        Configuration config = new Configuration();
        String restAddress = System.getenv().getOrDefault("FLINK_REST_ADDRESS", "flink-1762357787-jobmanager");
        int restPort = Integer.parseInt(System.getenv().getOrDefault("FLINK_REST_PORT", "8081"));
        config.set(RestOptions.ADDRESS, restAddress);
        config.set(RestOptions.PORT, restPort);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(config, FormPipeline.class.getClassLoader());
        env.setParallelism(1);
        return env;
    }

    private static void setNullableString(java.sql.PreparedStatement statement, int index, String value) throws java.sql.SQLException {
        if (value == null) {
            statement.setNull(index, Types.VARCHAR);
        } else {
            statement.setString(index, value);
        }
    }

    private static void setNullableInteger(java.sql.PreparedStatement statement, int index, Integer value) throws java.sql.SQLException {
        if (value == null) {
            statement.setNull(index, Types.INTEGER);
        } else {
            statement.setInt(index, value);
        }
    }

    private static void setNullableDouble(java.sql.PreparedStatement statement, int index, Double value) throws java.sql.SQLException {
        if (value == null) {
            statement.setNull(index, Types.DOUBLE);
        } else {
            statement.setDouble(index, value);
        }
    }

    private static void setNullableTimestamp(java.sql.PreparedStatement statement, int index, Timestamp value) throws java.sql.SQLException {
        if (value == null) {
            statement.setNull(index, Types.TIMESTAMP);
        } else {
            statement.setTimestamp(index, value);
        }
    }

    // Form POJO class
    public static class Form {
        private String orgName;
        private String orgINN;
        private String orgKPP;
        private String kodForm;
        private Timestamp period1;
        private Timestamp period2;
        private String kodstr;
        private Integer kodkol;
        private Double summa;
        private Timestamp loadTimestamp;

        // Getters and setters
        public String getOrgName() { return orgName; }
        public void setOrgName(String orgName) { this.orgName = orgName; }
        public String getOrgINN() { return orgINN; }
        public void setOrgINN(String orgINN) { this.orgINN = orgINN; }
        public String getOrgKPP() { return orgKPP; }
        public void setOrgKPP(String orgKPP) { this.orgKPP = orgKPP; }
        public String getKodForm() { return kodForm; }
        public void setKodForm(String kodForm) { this.kodForm = kodForm; }
        public Timestamp getPeriod1() { return period1; }
        public void setPeriod1(Timestamp period1) { this.period1 = period1; }
        public Timestamp getPeriod2() { return period2; }
        public void setPeriod2(Timestamp period2) { this.period2 = period2; }
        public String getKodstr() { return kodstr; }
        public void setKodstr(String kodstr) { this.kodstr = kodstr; }
        public Integer getKodkol() { return kodkol; }
        public void setKodkol(Integer kodkol) { this.kodkol = kodkol; }
        public Double getSumma() { return summa; }
        public void setSumma(Double summa) { this.summa = summa; }
        public Timestamp getLoadTimestamp() { return loadTimestamp; }
        public void setLoadTimestamp(Timestamp loadTimestamp) { this.loadTimestamp = loadTimestamp; }
    }
}

