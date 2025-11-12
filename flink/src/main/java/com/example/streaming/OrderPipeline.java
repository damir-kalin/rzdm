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

public class OrderPipeline {

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
                .setTopics("sys__isras__esud_rzdm__order__data")
                .setGroupId("flink-order-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Create data stream from Kafka
        DataStream<String> stream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Order Source")
                .name("Kafka Order Source")
                .uid("kafka-order-source");

        // Transform the JSON data - flatten orders and items
        DataStream<Order> eventStream = stream.flatMap(new FlatMapFunction<String, Order>() {
            @Override
            public void flatMap(String value, Collector<Order> out) throws Exception {
                if (value == null || value.trim().isEmpty()) {
                    System.err.println("Warning: Received null or empty message, skipping...");
                    return;
                }

                String trimmedValue = value.trim();
                JSONArray ordersToProcess = new JSONArray();

                try {
                    if (trimmedValue.startsWith("[")) {
                        ordersToProcess = new JSONArray(trimmedValue);
                        System.out.println("Parsed JSON array with " + ordersToProcess.length() + " orders");
                    } else if (trimmedValue.startsWith("{")) {
                        JSONObject json = new JSONObject(trimmedValue);
                        if (json.has("items")) {
                            ordersToProcess = json.getJSONArray("items");
                            System.out.println("Extracted " + ordersToProcess.length() + " orders from 'items' array");
                        } else {
                            ordersToProcess.put(json);
                            System.out.println("Processing single order object");
                        }
                    } else {
                        System.err.println("Warning: Invalid JSON format (doesn't start with [ or {): " +
                            (trimmedValue.length() > 100 ? trimmedValue.substring(0, 100) + "..." : trimmedValue));
                        return;
                    }
                } catch (org.json.JSONException e) {
                    System.err.println("Error parsing JSON: " + e.getMessage());
                    System.err.println("Error position: " + e.getMessage());
                    // Try to find the problematic line
                    if (trimmedValue.length() > 500) {
                        System.err.println("Problematic message (first 500 chars): " + trimmedValue.substring(0, 500) + "...");
                    } else {
                        System.err.println("Problematic message: " + trimmedValue);
                    }
                    // Log full stack trace for debugging
                    e.printStackTrace();
                    return;
                } catch (Exception e) {
                    System.err.println("Unexpected error parsing JSON: " + e.getMessage());
                    e.printStackTrace();
                    return;
                }

                // Process each order
                for (int i = 0; i < ordersToProcess.length(); i++) {
                    try {
                        JSONObject orderJson = ordersToProcess.getJSONObject(i);
                    
                    // Extract order level fields
                    String orderId = safeParseString(orderJson, "order_id");
                    Timestamp orderDate = parseDateTime(orderJson, "order_date");
                    
                    // Extract organization
                    JSONObject orgJson = orderJson.optJSONObject("organization");
                    String orgINN = orgJson != null ? safeParseString(orgJson, "org_inn") : null;
                    String orgKPP = orgJson != null ? safeParseString(orgJson, "org_kpp") : null;
                    String orgName = orgJson != null ? safeParseString(orgJson, "org_name") : null;
                    String orgMDMKey = orgJson != null ? safeParseString(orgJson, "org_mdmkey") : null;
                    
                    // Extract department
                    JSONObject deptJson = orderJson.optJSONObject("org_department");
                    String departCode = deptJson != null ? safeParseString(deptJson, "depart_code") : null;
                    String departName = deptJson != null ? safeParseString(deptJson, "depart_name") : null;
                    String departMDMKey = deptJson != null ? safeParseString(deptJson, "depart_mdmkey") : null;
                    
                    String patientCode = safeParseString(orderJson, "patient_code");
                    String orderCod = safeParseString(orderJson, "order_cod");
                    
                    // Extract doctor
                    JSONObject doctorJson = orderJson.optJSONObject("doctor");
                    String doctorCode = doctorJson != null ? safeParseString(doctorJson, "doctor_code") : null;
                    String doctorName = doctorJson != null ? safeParseString(doctorJson, "doctor_name") : null;
                    
                    // Extract store
                    JSONObject storeJson = orderJson.optJSONObject("tostore");
                    String tostoreMDMKey = storeJson != null ? safeParseString(storeJson, "tostore_mdmkey") : null;
                    String tostoreName = storeJson != null ? safeParseString(storeJson, "tostore_name") : null;
                    
                    // Process items - goods
                    JSONObject itemsJson = orderJson.optJSONObject("items");
                    boolean hasItems = false;
                    
                    if (itemsJson != null) {
                        JSONArray goodsArray = itemsJson.optJSONArray("goods");
                        if (goodsArray != null && goodsArray.length() > 0) {
                            for (int j = 0; j < goodsArray.length(); j++) {
                                try {
                                    JSONObject goodJson = goodsArray.getJSONObject(j);
                                    // Skip if all goods fields are empty
                                    String idCpz = safeParseString(goodJson, "id_cpz");
                                    String goodsName = safeParseString(goodJson, "goods_name");
                                    String goodsQntyStr = safeParseString(goodJson, "goods_qnty");
                                    
                                    if (idCpz == null && goodsName == null && (goodsQntyStr == null || goodsQntyStr.trim().isEmpty())) {
                                        System.out.println("Skipping empty goods item in order: " + orderId);
                                        continue;
                                    }
                                    
                                    Order order = new Order();
                                    
                                    order.setOrderId(orderId);
                                    order.setOrderDate(orderDate);
                                    order.setOrgINN(orgINN);
                                    order.setOrgKPP(orgKPP);
                                    order.setOrgName(orgName);
                                    order.setOrgMDMKey(orgMDMKey);
                                    order.setDepartCode(departCode);
                                    order.setDepartName(departName);
                                    order.setDepartMDMKey(departMDMKey);
                                    order.setPatientCode(patientCode);
                                    order.setOrderCod(orderCod);
                                    order.setDoctorCode(doctorCode);
                                    order.setDoctorName(doctorName);
                                    order.setTostoreMDMKey(tostoreMDMKey);
                                    order.setTostoreName(tostoreName);
                                    order.setItemType("goods");
                                    order.setIdCpz(idCpz);
                                    order.setGoodsName(goodsName);
                                    order.setGoodsQnty(parseInteger(goodsQntyStr));
                                    order.setLoadTimestamp(Timestamp.valueOf(LocalDateTime.now()));
                                    
                                    out.collect(order);
                                    hasItems = true;
                                } catch (Exception e) {
                                    System.err.println("Error processing goods item " + j + " in order " + orderId + ": " + e.getMessage());
                                }
                            }
                        }
                        
                        // Process drugs
                        JSONArray drugsArray = itemsJson.optJSONArray("drugs");
                        if (drugsArray != null && drugsArray.length() > 0) {
                            for (int j = 0; j < drugsArray.length(); j++) {
                                try {
                                    JSONObject drugJson = drugsArray.getJSONObject(j);
                                    // Skip if all drug fields are empty
                                    String drugMnn = safeParseString(drugJson, "drug_mnn");
                                    String drugDoze = safeParseString(drugJson, "drug_doze");
                                    String drugForm = safeParseString(drugJson, "drug_form");
                                    String drugQnty = safeParseString(drugJson, "drug_qnty");
                                    
                                    if ((drugMnn == null || drugMnn.trim().isEmpty()) &&
                                        (drugDoze == null || drugDoze.trim().isEmpty()) &&
                                        (drugForm == null || drugForm.trim().isEmpty()) &&
                                        (drugQnty == null || drugQnty.trim().isEmpty())) {
                                        System.out.println("Skipping empty drugs item in order: " + orderId);
                                        continue;
                                    }
                                    
                                    Order order = new Order();
                                    
                                    order.setOrderId(orderId);
                                    order.setOrderDate(orderDate);
                                    order.setOrgINN(orgINN);
                                    order.setOrgKPP(orgKPP);
                                    order.setOrgName(orgName);
                                    order.setOrgMDMKey(orgMDMKey);
                                    order.setDepartCode(departCode);
                                    order.setDepartName(departName);
                                    order.setDepartMDMKey(departMDMKey);
                                    order.setPatientCode(patientCode);
                                    order.setOrderCod(orderCod);
                                    order.setDoctorCode(doctorCode);
                                    order.setDoctorName(doctorName);
                                    order.setTostoreMDMKey(tostoreMDMKey);
                                    order.setTostoreName(tostoreName);
                                    order.setItemType("drugs");
                                    order.setDrugMnn(drugMnn);
                                    order.setDrugDoze(drugDoze);
                                    order.setDrugForm(drugForm);
                                    order.setDrugQnty(drugQnty);
                                    order.setLoadTimestamp(Timestamp.valueOf(LocalDateTime.now()));
                                    
                                    out.collect(order);
                                    hasItems = true;
                                } catch (Exception e) {
                                    System.err.println("Error processing drugs item " + j + " in order " + orderId + ": " + e.getMessage());
                                }
                            }
                        }
                    }
                    
                    // Если нет items или items пуст, создаем запись без позиций
                    if (!hasItems && orderId != null && !orderId.trim().isEmpty()) {
                        Order order = new Order();
                        order.setOrderId(orderId);
                        order.setOrderDate(orderDate);
                        order.setOrgINN(orgINN);
                        order.setOrgKPP(orgKPP);
                        order.setOrgName(orgName);
                        order.setOrgMDMKey(orgMDMKey);
                        order.setDepartCode(departCode);
                        order.setDepartName(departName);
                        order.setDepartMDMKey(departMDMKey);
                        order.setPatientCode(patientCode);
                        order.setOrderCod(orderCod);
                        order.setDoctorCode(doctorCode);
                        order.setDoctorName(doctorName);
                        order.setTostoreMDMKey(tostoreMDMKey);
                        order.setTostoreName(tostoreName);
                        order.setItemType(null);
                        order.setLoadTimestamp(Timestamp.valueOf(LocalDateTime.now()));
                        
                        out.collect(order);
                    }
                    } catch (Exception e) {
                        System.err.println("Error processing order " + i + ": " + e.getMessage());
                        e.printStackTrace();
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

            private Timestamp parseDateTime(JSONObject json, String key) {
                String value = safeParseString(json, key);
                if (value == null || value.isEmpty()) return null;
                try {
                    // Format: "12.11.2025 11:27:37"
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");
                    LocalDateTime dateTime = LocalDateTime.parse(value, formatter);
                    return Timestamp.valueOf(dateTime);
                } catch (Exception e) {
                    return null;
                }
            }
        }).name("Parse order payload")
          .uid("parse-orders");

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

        final int parameterCount = 24;

        String insertSql = "INSERT INTO iceberg.rzdm_test.stg_orders (" +
                    "order_id, order_date, org_inn, org_kpp, org_name, org_mdmkey, " +
                    "depart_code, depart_name, depart_mdmkey, patient_code, order_cod, " +
                    "doctor_code, doctor_name, tostore_mdmkey, tostore_name, " +
                    "item_type, id_cpz, goods_name, goods_qnty, " +
                    "drug_mnn, drug_doze, drug_form, drug_qnty, load_timestamp" +
                ") VALUES (" +
                    String.join(", ", Collections.nCopies(parameterCount, "?")) +
                ")";

        JdbcStatementBuilder<Order> statementBuilder = (statement, order) -> {
            int index = 1;
            setNullableString(statement, index++, order.getOrderId());
            setNullableTimestamp(statement, index++, order.getOrderDate());
            setNullableString(statement, index++, order.getOrgINN());
            setNullableString(statement, index++, order.getOrgKPP());
            setNullableString(statement, index++, order.getOrgName());
            setNullableString(statement, index++, order.getOrgMDMKey());
            setNullableString(statement, index++, order.getDepartCode());
            setNullableString(statement, index++, order.getDepartName());
            setNullableString(statement, index++, order.getDepartMDMKey());
            setNullableString(statement, index++, order.getPatientCode());
            setNullableString(statement, index++, order.getOrderCod());
            setNullableString(statement, index++, order.getDoctorCode());
            setNullableString(statement, index++, order.getDoctorName());
            setNullableString(statement, index++, order.getTostoreMDMKey());
            setNullableString(statement, index++, order.getTostoreName());
            setNullableString(statement, index++, order.getItemType());
            setNullableString(statement, index++, order.getIdCpz());
            setNullableString(statement, index++, order.getGoodsName());
            setNullableInteger(statement, index++, order.getGoodsQnty());
            setNullableString(statement, index++, order.getDrugMnn());
            setNullableString(statement, index++, order.getDrugDoze());
            setNullableString(statement, index++, order.getDrugForm());
            setNullableString(statement, index++, order.getDrugQnty());
            setNullableTimestamp(statement, index++, order.getLoadTimestamp());
        };

        DataStream<Order> filteredStream = eventStream
                .filter(Objects::nonNull)
                .name("Filter null orders")
                .uid("filter-orders");

        JdbcSink<Order> starRocksSink = JdbcSink.<Order>builder()
                .withQueryStatement(insertSql, statementBuilder)
                .withExecutionOptions(executionOptions)
                .buildAtLeastOnce(connectionOptions);

        filteredStream
                .sinkTo(starRocksSink)
                .name("StarRocks JDBC Sink")
                .uid("order-jdbc-sink")
                .setParallelism(1);

        env.execute("Order Streaming Pipeline");
    }

    private static StreamExecutionEnvironment createExecutionEnvironment() {
        Configuration config = new Configuration();
        String restAddress = System.getenv().getOrDefault("FLINK_REST_ADDRESS", "flink-1762357787-jobmanager");
        int restPort = Integer.parseInt(System.getenv().getOrDefault("FLINK_REST_PORT", "8081"));
        config.set(RestOptions.ADDRESS, restAddress);
        config.set(RestOptions.PORT, restPort);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(config, OrderPipeline.class.getClassLoader());
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

    private static void setNullableTimestamp(java.sql.PreparedStatement statement, int index, Timestamp value) throws java.sql.SQLException {
        if (value == null) {
            statement.setNull(index, Types.TIMESTAMP);
        } else {
            statement.setTimestamp(index, value);
        }
    }

    // Order POJO class
    public static class Order {
        private String orderId;
        private Timestamp orderDate;
        private String orgINN;
        private String orgKPP;
        private String orgName;
        private String orgMDMKey;
        private String departCode;
        private String departName;
        private String departMDMKey;
        private String patientCode;
        private String orderCod;
        private String doctorCode;
        private String doctorName;
        private String tostoreMDMKey;
        private String tostoreName;
        private String itemType;
        private String idCpz;
        private String goodsName;
        private Integer goodsQnty;
        private String drugMnn;
        private String drugDoze;
        private String drugForm;
        private String drugQnty;
        private Timestamp loadTimestamp;

        // Getters and setters
        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public Timestamp getOrderDate() { return orderDate; }
        public void setOrderDate(Timestamp orderDate) { this.orderDate = orderDate; }
        public String getOrgINN() { return orgINN; }
        public void setOrgINN(String orgINN) { this.orgINN = orgINN; }
        public String getOrgKPP() { return orgKPP; }
        public void setOrgKPP(String orgKPP) { this.orgKPP = orgKPP; }
        public String getOrgName() { return orgName; }
        public void setOrgName(String orgName) { this.orgName = orgName; }
        public String getOrgMDMKey() { return orgMDMKey; }
        public void setOrgMDMKey(String orgMDMKey) { this.orgMDMKey = orgMDMKey; }
        public String getDepartCode() { return departCode; }
        public void setDepartCode(String departCode) { this.departCode = departCode; }
        public String getDepartName() { return departName; }
        public void setDepartName(String departName) { this.departName = departName; }
        public String getDepartMDMKey() { return departMDMKey; }
        public void setDepartMDMKey(String departMDMKey) { this.departMDMKey = departMDMKey; }
        public String getPatientCode() { return patientCode; }
        public void setPatientCode(String patientCode) { this.patientCode = patientCode; }
        public String getOrderCod() { return orderCod; }
        public void setOrderCod(String orderCod) { this.orderCod = orderCod; }
        public String getDoctorCode() { return doctorCode; }
        public void setDoctorCode(String doctorCode) { this.doctorCode = doctorCode; }
        public String getDoctorName() { return doctorName; }
        public void setDoctorName(String doctorName) { this.doctorName = doctorName; }
        public String getTostoreMDMKey() { return tostoreMDMKey; }
        public void setTostoreMDMKey(String tostoreMDMKey) { this.tostoreMDMKey = tostoreMDMKey; }
        public String getTostoreName() { return tostoreName; }
        public void setTostoreName(String tostoreName) { this.tostoreName = tostoreName; }
        public String getItemType() { return itemType; }
        public void setItemType(String itemType) { this.itemType = itemType; }
        public String getIdCpz() { return idCpz; }
        public void setIdCpz(String idCpz) { this.idCpz = idCpz; }
        public String getGoodsName() { return goodsName; }
        public void setGoodsName(String goodsName) { this.goodsName = goodsName; }
        public Integer getGoodsQnty() { return goodsQnty; }
        public void setGoodsQnty(Integer goodsQnty) { this.goodsQnty = goodsQnty; }
        public String getDrugMnn() { return drugMnn; }
        public void setDrugMnn(String drugMnn) { this.drugMnn = drugMnn; }
        public String getDrugDoze() { return drugDoze; }
        public void setDrugDoze(String drugDoze) { this.drugDoze = drugDoze; }
        public String getDrugForm() { return drugForm; }
        public void setDrugForm(String drugForm) { this.drugForm = drugForm; }
        public String getDrugQnty() { return drugQnty; }
        public void setDrugQnty(String drugQnty) { this.drugQnty = drugQnty; }
        public Timestamp getLoadTimestamp() { return loadTimestamp; }
        public void setLoadTimestamp(Timestamp loadTimestamp) { this.loadTimestamp = loadTimestamp; }
    }
}

