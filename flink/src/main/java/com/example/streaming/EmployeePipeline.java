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
import java.sql.Types;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Objects;

public class EmployeePipeline {

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
                .setTopics("sys__kuirzp__esud_rzdm__employee__data")
                .setGroupId("flink-employee-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Create data stream from Kafka
        DataStream<String> stream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Employee Source")
                .name("Kafka Employee Source")
                .uid("kafka-employee-source");

        // Transform the JSON data
        DataStream<Employee> eventStream = stream.flatMap(new FlatMapFunction<String, Employee>() {
            @Override
            public void flatMap(String value, Collector<Employee> out) throws Exception {
                if (value == null || value.trim().isEmpty()) {
                    System.err.println("Warning: Received null or empty message, skipping...");
                    return;
                }

                String trimmedValue = value.trim();
                org.json.JSONArray itemsToProcess = new org.json.JSONArray();

                try {
                    if (trimmedValue.startsWith("[")) {
                        org.json.JSONArray jsonArray = new org.json.JSONArray(trimmedValue);
                        if (jsonArray.length() > 0) {
                            org.json.JSONObject firstElement = jsonArray.getJSONObject(0);
                            if (firstElement.has("items")) {
                                itemsToProcess = firstElement.getJSONArray("items");
                                System.out.println("Extracting " + itemsToProcess.length() + " items from items array");
                            } else {
                                itemsToProcess = jsonArray;
                            }
                        } else {
                            System.err.println("Warning: JSON array is empty");
                            return;
                        }
                    } else if (trimmedValue.startsWith("{")) {
                        JSONObject json = new JSONObject(trimmedValue);
                        if (json.has("items")) {
                            itemsToProcess = json.getJSONArray("items");
                        } else {
                            itemsToProcess.put(json);
                        }
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

                // Process each item
                for (int i = 0; i < itemsToProcess.length(); i++) {
                    JSONObject json = itemsToProcess.getJSONObject(i);
                    Employee employee = new Employee();

                    employee.setOrganization(safeParseString(json, "Organization"));
                    employee.setOrganizationINN(safeParseString(json, "OrganizationINN"));
                    employee.setOrganizationKPP(safeParseString(json, "OrganizationKPP"));
                    employee.setOrganizationMDMKey(safeParseString(json, "OrganizationMDMKey"));
                    employee.setDivision(safeParseString(json, "Division"));
                    employee.setDivisionMDMKey(safeParseString(json, "DivisionMDMKey"));
                    employee.setEmployee(safeParseString(json, "Employee"));
                    employee.setTableNumber(safeParseString(json, "TableNumber"));
                    employee.setPost(safeParseString(json, "Post"));
                    employee.setGender(safeParseString(json, "Gender"));
                    employee.setTypeOfEmployment(safeParseString(json, "TypeOfEmployment"));
                    employee.setTimeStamp(parseDateTime(json, "TimeStamp"));
                    employee.setLoadTimestamp(Timestamp.valueOf(LocalDateTime.now()));

                    out.collect(employee);
                }
            }

            private String safeParseString(JSONObject json, String key) {
                if (!json.has(key) || json.isNull(key)) return null;
                try {
                    Object value = json.get(key);
                    if (value == null) return null;
                    return value.toString();
                } catch (Exception e) {
                    return null;
                }
            }

            private Timestamp parseDateTime(JSONObject json, String key) {
                String value = safeParseString(json, key);
                if (value == null || value.isEmpty()) return null;
                try {
                    // Format: "12.11.2025 14:16:25"
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");
                    LocalDateTime dateTime = LocalDateTime.parse(value, formatter);
                    return Timestamp.valueOf(dateTime);
                } catch (Exception e) {
                    return null;
                }
            }
        }).name("Parse employee payload")
          .uid("parse-employees");

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

        final int parameterCount = 13;

        String insertSql = "INSERT INTO iceberg.rzdm_test.stg_employees (" +
                    "Organization, OrganizationINN, OrganizationKPP, OrganizationMDMKey, " +
                    "Division, DivisionMDMKey, Employee, TableNumber, Post, " +
                    "Gender, TypeOfEmployment, TimeStamp, load_timestamp" +
                ") VALUES (" +
                    String.join(", ", Collections.nCopies(parameterCount, "?")) +
                ")";

        JdbcStatementBuilder<Employee> statementBuilder = (statement, employee) -> {
            int index = 1;
            setNullableString(statement, index++, employee.getOrganization());
            setNullableString(statement, index++, employee.getOrganizationINN());
            setNullableString(statement, index++, employee.getOrganizationKPP());
            setNullableString(statement, index++, employee.getOrganizationMDMKey());
            setNullableString(statement, index++, employee.getDivision());
            setNullableString(statement, index++, employee.getDivisionMDMKey());
            setNullableString(statement, index++, employee.getEmployee());
            setNullableString(statement, index++, employee.getTableNumber());
            setNullableString(statement, index++, employee.getPost());
            setNullableString(statement, index++, employee.getGender());
            setNullableString(statement, index++, employee.getTypeOfEmployment());
            setNullableTimestamp(statement, index++, employee.getTimeStamp());
            setNullableTimestamp(statement, index++, employee.getLoadTimestamp());
        };

        DataStream<Employee> filteredStream = eventStream
                .filter(Objects::nonNull)
                .name("Filter null employees")
                .uid("filter-employees");

        JdbcSink<Employee> starRocksSink = JdbcSink.<Employee>builder()
                .withQueryStatement(insertSql, statementBuilder)
                .withExecutionOptions(executionOptions)
                .buildAtLeastOnce(connectionOptions);

        filteredStream
                .sinkTo(starRocksSink)
                .name("StarRocks JDBC Sink")
                .uid("employee-jdbc-sink")
                .setParallelism(1);

        env.execute("Employee Streaming Pipeline");
    }

    private static StreamExecutionEnvironment createExecutionEnvironment() {
        Configuration config = new Configuration();
        String restAddress = System.getenv().getOrDefault("FLINK_REST_ADDRESS", "flink-1762357787-jobmanager");
        int restPort = Integer.parseInt(System.getenv().getOrDefault("FLINK_REST_PORT", "8081"));
        config.set(RestOptions.ADDRESS, restAddress);
        config.set(RestOptions.PORT, restPort);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(config, EmployeePipeline.class.getClassLoader());
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

    private static void setNullableTimestamp(java.sql.PreparedStatement statement, int index, Timestamp value) throws java.sql.SQLException {
        if (value == null) {
            statement.setNull(index, Types.TIMESTAMP);
        } else {
            statement.setTimestamp(index, value);
        }
    }

    // Employee POJO class
    public static class Employee {
        private String Organization;
        private String OrganizationINN;
        private String OrganizationKPP;
        private String OrganizationMDMKey;
        private String Division;
        private String DivisionMDMKey;
        private String Employee;
        private String TableNumber;
        private String Post;
        private String Gender;
        private String TypeOfEmployment;
        private Timestamp TimeStamp;
        private Timestamp LoadTimestamp;

        // Getters and setters
        public String getOrganization() { return Organization; }
        public void setOrganization(String Organization) { this.Organization = Organization; }
        public String getOrganizationINN() { return OrganizationINN; }
        public void setOrganizationINN(String OrganizationINN) { this.OrganizationINN = OrganizationINN; }
        public String getOrganizationKPP() { return OrganizationKPP; }
        public void setOrganizationKPP(String OrganizationKPP) { this.OrganizationKPP = OrganizationKPP; }
        public String getOrganizationMDMKey() { return OrganizationMDMKey; }
        public void setOrganizationMDMKey(String OrganizationMDMKey) { this.OrganizationMDMKey = OrganizationMDMKey; }
        public String getDivision() { return Division; }
        public void setDivision(String Division) { this.Division = Division; }
        public String getDivisionMDMKey() { return DivisionMDMKey; }
        public void setDivisionMDMKey(String DivisionMDMKey) { this.DivisionMDMKey = DivisionMDMKey; }
        public String getEmployee() { return Employee; }
        public void setEmployee(String Employee) { this.Employee = Employee; }
        public String getTableNumber() { return TableNumber; }
        public void setTableNumber(String TableNumber) { this.TableNumber = TableNumber; }
        public String getPost() { return Post; }
        public void setPost(String Post) { this.Post = Post; }
        public String getGender() { return Gender; }
        public void setGender(String Gender) { this.Gender = Gender; }
        public String getTypeOfEmployment() { return TypeOfEmployment; }
        public void setTypeOfEmployment(String TypeOfEmployment) { this.TypeOfEmployment = TypeOfEmployment; }
        public Timestamp getTimeStamp() { return TimeStamp; }
        public void setTimeStamp(Timestamp TimeStamp) { this.TimeStamp = TimeStamp; }
        public Timestamp getLoadTimestamp() { return LoadTimestamp; }
        public void setLoadTimestamp(Timestamp LoadTimestamp) { this.LoadTimestamp = LoadTimestamp; }
    }
}


