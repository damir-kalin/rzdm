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

public class MovementPipeline {

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
                .setTopics("sys__isras__esud_rzdm__movement__data")
                .setGroupId("flink-movement-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Create data stream from Kafka
        DataStream<String> stream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Movement Source")
                .name("Kafka Movement Source")
                .uid("kafka-movement-source");

        // Transform the JSON data - flatten movements and items
        DataStream<Movement> eventStream = stream.flatMap(new FlatMapFunction<String, Movement>() {
            @Override
            public void flatMap(String value, Collector<Movement> out) throws Exception {
                if (value == null || value.trim().isEmpty()) {
                    System.err.println("Warning: Received null or empty message, skipping...");
                    return;
                }

                String trimmedValue = value.trim();
                JSONArray movementsToProcess = new JSONArray();

                try {
                    if (trimmedValue.startsWith("[")) {
                        movementsToProcess = new JSONArray(trimmedValue);
                    } else if (trimmedValue.startsWith("{")) {
                        JSONObject json = new JSONObject(trimmedValue);
                        if (json.has("items")) {
                            movementsToProcess = json.getJSONArray("items");
                        } else {
                            movementsToProcess.put(json);
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

                // Process each movement
                for (int i = 0; i < movementsToProcess.length(); i++) {
                    JSONObject movementJson = movementsToProcess.getJSONObject(i);
                    
                    // Extract movement level fields
                    String transferIsrasId = safeParseString(movementJson, "transfer_isras_id");
                    String transferMisId = safeParseString(movementJson, "transfer_mis_id");
                    
                    // Extract organization
                    JSONObject orgJson = movementJson.optJSONObject("organization");
                    String orgINN = orgJson != null ? safeParseString(orgJson, "org_inn") : null;
                    String orgKPP = orgJson != null ? safeParseString(orgJson, "org_kpp") : null;
                    String orgName = orgJson != null ? safeParseString(orgJson, "org_name") : null;
                    String orgMDMKey = orgJson != null ? safeParseString(orgJson, "org_mdmkey") : null;
                    
                    // Extract comment
                    JSONObject commentJson = movementJson.optJSONObject("comment");
                    String transferNum = commentJson != null ? safeParseString(commentJson, "transfer_num") : null;
                    Timestamp transferDate = commentJson != null ? parseDateTime(commentJson, "transfer_date") : null;
                    
                    // Extract fromstore
                    JSONObject fromstoreJson = movementJson.optJSONObject("fromstore");
                    String fromstoreMDMKey = fromstoreJson != null ? safeParseString(fromstoreJson, "fromstore_mdmkey") : null;
                    String fromstoreName = fromstoreJson != null ? safeParseString(fromstoreJson, "fromstore_name") : null;
                    
                    // Extract tostore
                    JSONObject tostoreJson = movementJson.optJSONObject("tostore");
                    String tostoreMDMKey = tostoreJson != null ? safeParseString(tostoreJson, "tostore_mdmkey") : null;
                    String tostoreName = tostoreJson != null ? safeParseString(tostoreJson, "tostore_name") : null;
                    
                    // Extract transfer_finsource
                    JSONObject finsourceJson = movementJson.optJSONObject("transfer_finsource");
                    String transferFinsourceMDMKey = finsourceJson != null ? safeParseString(finsourceJson, "transfer_finsource_mdmkey") : null;
                    String transferFinsourceName = finsourceJson != null ? safeParseString(finsourceJson, "transfer_finsource_name") : null;
                    
                    String transferSourcedocNum = safeParseString(movementJson, "transfer_sourcedoc_num");
                    Timestamp transferSourcedocDate = parseDateTime(movementJson, "transfer_sourcedoc_date");
                    
                    // Process items
                    JSONArray itemsArray = movementJson.optJSONArray("items");
                    if (itemsArray != null) {
                        for (int j = 0; j < itemsArray.length(); j++) {
                            JSONObject itemJson = itemsArray.getJSONObject(j);
                            Movement movement = new Movement();
                            
                            movement.setTransferIsrasId(transferIsrasId);
                            movement.setTransferMisId(transferMisId);
                            movement.setOrgINN(orgINN);
                            movement.setOrgKPP(orgKPP);
                            movement.setOrgName(orgName);
                            movement.setOrgMDMKey(orgMDMKey);
                            movement.setTransferNum(transferNum);
                            movement.setTransferDate(transferDate);
                            movement.setFromstoreMDMKey(fromstoreMDMKey);
                            movement.setFromstoreName(fromstoreName);
                            movement.setTostoreMDMKey(tostoreMDMKey);
                            movement.setTostoreName(tostoreName);
                            movement.setTransferFinsourceMDMKey(transferFinsourceMDMKey);
                            movement.setTransferFinsourceName(transferFinsourceName);
                            movement.setTransferSourcedocNum(transferSourcedocNum);
                            movement.setTransferSourcedocDate(transferSourcedocDate);
                            
                            movement.setIdCpz(safeParseString(itemJson, "id_cpz"));
                            movement.setGoodsName(safeParseString(itemJson, "goods_name"));
                            movement.setGoodsQnty(parseInteger(safeParseString(itemJson, "goods_qnty")));
                            movement.setGoodsSn(safeParseString(itemJson, "goods_sn"));
                            movement.setGoodsExpdate(parseDateTime(itemJson, "goods_expdate"));
                            movement.setGoodsPrice(parseDouble(safeParseString(itemJson, "goods_price")));
                            movement.setGoodsMesunit(safeParseString(itemJson, "goods_mesunit"));
                            movement.setGoodsBarcode(safeParseString(itemJson, "goods_barcode"));
                            movement.setGoodsSum(parseDouble(safeParseString(itemJson, "goods_sum")));
                            movement.setGoodsSumnds(parseDouble(safeParseString(itemJson, "goods_sumnds")));
                            movement.setGoodsNdspercent(safeParseString(itemJson, "goods_ndspercent"));
                            movement.setGoodsProducerprice(parseDouble(safeParseString(itemJson, "goods_producerprice")));
                            
                            movement.setLoadTimestamp(Timestamp.valueOf(LocalDateTime.now()));
                            
                            out.collect(movement);
                        }
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

            private Double parseDouble(String value) {
                if (value == null || value.trim().isEmpty()) return null;
                try {
                    // Remove spaces and replace comma with dot
                    String cleaned = value.replaceAll("[\\s,]", "").replace(",", ".");
                    return Double.parseDouble(cleaned);
                } catch (Exception e) {
                    return null;
                }
            }

            private Timestamp parseDateTime(JSONObject json, String key) {
                String value = safeParseString(json, key);
                if (value == null || value.isEmpty()) return null;
                try {
                    // Format: "26.11.2025 3:06:50" or "24.02.2025 11:41:30" or "30.09.2026 0:00:00"
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy H:mm:ss");
                    LocalDateTime dateTime = LocalDateTime.parse(value, formatter);
                    return Timestamp.valueOf(dateTime);
                } catch (Exception e) {
                    // Try alternative format
                    try {
                        DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");
                        LocalDateTime dateTime = LocalDateTime.parse(value, formatter2);
                        return Timestamp.valueOf(dateTime);
                    } catch (Exception e2) {
                        return null;
                    }
                }
            }
        }).name("Parse movement payload")
          .uid("parse-movements");

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

        final int parameterCount = 29;

        String insertSql = "INSERT INTO iceberg.rzdm_test.stg_movements (" +
                    "transfer_isras_id, transfer_mis_id, org_inn, org_kpp, org_name, org_mdmkey, " +
                    "transfer_num, transfer_date, fromstore_mdmkey, fromstore_name, " +
                    "tostore_mdmkey, tostore_name, transfer_finsource_mdmkey, transfer_finsource_name, " +
                    "transfer_sourcedoc_num, transfer_sourcedoc_date, " +
                    "id_cpz, goods_name, goods_qnty, goods_sn, goods_expdate, " +
                    "goods_price, goods_mesunit, goods_barcode, goods_sum, goods_sumnds, " +
                    "goods_ndspercent, goods_producerprice, load_timestamp" +
                ") VALUES (" +
                    String.join(", ", Collections.nCopies(parameterCount, "?")) +
                ")";

        JdbcStatementBuilder<Movement> statementBuilder = (statement, movement) -> {
            int index = 1;
            setNullableString(statement, index++, movement.getTransferIsrasId());
            setNullableString(statement, index++, movement.getTransferMisId());
            setNullableString(statement, index++, movement.getOrgINN());
            setNullableString(statement, index++, movement.getOrgKPP());
            setNullableString(statement, index++, movement.getOrgName());
            setNullableString(statement, index++, movement.getOrgMDMKey());
            setNullableString(statement, index++, movement.getTransferNum());
            setNullableTimestamp(statement, index++, movement.getTransferDate());
            setNullableString(statement, index++, movement.getFromstoreMDMKey());
            setNullableString(statement, index++, movement.getFromstoreName());
            setNullableString(statement, index++, movement.getTostoreMDMKey());
            setNullableString(statement, index++, movement.getTostoreName());
            setNullableString(statement, index++, movement.getTransferFinsourceMDMKey());
            setNullableString(statement, index++, movement.getTransferFinsourceName());
            setNullableString(statement, index++, movement.getTransferSourcedocNum());
            setNullableTimestamp(statement, index++, movement.getTransferSourcedocDate());
            setNullableString(statement, index++, movement.getIdCpz());
            setNullableString(statement, index++, movement.getGoodsName());
            setNullableInteger(statement, index++, movement.getGoodsQnty());
            setNullableString(statement, index++, movement.getGoodsSn());
            setNullableTimestamp(statement, index++, movement.getGoodsExpdate());
            setNullableDouble(statement, index++, movement.getGoodsPrice());
            setNullableString(statement, index++, movement.getGoodsMesunit());
            setNullableString(statement, index++, movement.getGoodsBarcode());
            setNullableDouble(statement, index++, movement.getGoodsSum());
            setNullableDouble(statement, index++, movement.getGoodsSumnds());
            setNullableString(statement, index++, movement.getGoodsNdspercent());
            setNullableDouble(statement, index++, movement.getGoodsProducerprice());
            setNullableTimestamp(statement, index++, movement.getLoadTimestamp());
        };

        DataStream<Movement> filteredStream = eventStream
                .filter(Objects::nonNull)
                .name("Filter null movements")
                .uid("filter-movements");

        JdbcSink<Movement> starRocksSink = JdbcSink.<Movement>builder()
                .withQueryStatement(insertSql, statementBuilder)
                .withExecutionOptions(executionOptions)
                .buildAtLeastOnce(connectionOptions);

        filteredStream
                .sinkTo(starRocksSink)
                .name("StarRocks JDBC Sink")
                .uid("movement-jdbc-sink")
                .setParallelism(1);

        env.execute("Movement Streaming Pipeline");
    }

    private static StreamExecutionEnvironment createExecutionEnvironment() {
        Configuration config = new Configuration();
        String restAddress = System.getenv().getOrDefault("FLINK_REST_ADDRESS", "flink-1762357787-jobmanager");
        int restPort = Integer.parseInt(System.getenv().getOrDefault("FLINK_REST_PORT", "8081"));
        config.set(RestOptions.ADDRESS, restAddress);
        config.set(RestOptions.PORT, restPort);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(config, MovementPipeline.class.getClassLoader());
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

    // Movement POJO class
    public static class Movement {
        private String transferIsrasId;
        private String transferMisId;
        private String orgINN;
        private String orgKPP;
        private String orgName;
        private String orgMDMKey;
        private String transferNum;
        private Timestamp transferDate;
        private String fromstoreMDMKey;
        private String fromstoreName;
        private String tostoreMDMKey;
        private String tostoreName;
        private String transferFinsourceMDMKey;
        private String transferFinsourceName;
        private String transferSourcedocNum;
        private Timestamp transferSourcedocDate;
        private String idCpz;
        private String goodsName;
        private Integer goodsQnty;
        private String goodsSn;
        private Timestamp goodsExpdate;
        private Double goodsPrice;
        private String goodsMesunit;
        private String goodsBarcode;
        private Double goodsSum;
        private Double goodsSumnds;
        private String goodsNdspercent;
        private Double goodsProducerprice;
        private Timestamp loadTimestamp;

        // Getters and setters
        public String getTransferIsrasId() { return transferIsrasId; }
        public void setTransferIsrasId(String transferIsrasId) { this.transferIsrasId = transferIsrasId; }
        public String getTransferMisId() { return transferMisId; }
        public void setTransferMisId(String transferMisId) { this.transferMisId = transferMisId; }
        public String getOrgINN() { return orgINN; }
        public void setOrgINN(String orgINN) { this.orgINN = orgINN; }
        public String getOrgKPP() { return orgKPP; }
        public void setOrgKPP(String orgKPP) { this.orgKPP = orgKPP; }
        public String getOrgName() { return orgName; }
        public void setOrgName(String orgName) { this.orgName = orgName; }
        public String getOrgMDMKey() { return orgMDMKey; }
        public void setOrgMDMKey(String orgMDMKey) { this.orgMDMKey = orgMDMKey; }
        public String getTransferNum() { return transferNum; }
        public void setTransferNum(String transferNum) { this.transferNum = transferNum; }
        public Timestamp getTransferDate() { return transferDate; }
        public void setTransferDate(Timestamp transferDate) { this.transferDate = transferDate; }
        public String getFromstoreMDMKey() { return fromstoreMDMKey; }
        public void setFromstoreMDMKey(String fromstoreMDMKey) { this.fromstoreMDMKey = fromstoreMDMKey; }
        public String getFromstoreName() { return fromstoreName; }
        public void setFromstoreName(String fromstoreName) { this.fromstoreName = fromstoreName; }
        public String getTostoreMDMKey() { return tostoreMDMKey; }
        public void setTostoreMDMKey(String tostoreMDMKey) { this.tostoreMDMKey = tostoreMDMKey; }
        public String getTostoreName() { return tostoreName; }
        public void setTostoreName(String tostoreName) { this.tostoreName = tostoreName; }
        public String getTransferFinsourceMDMKey() { return transferFinsourceMDMKey; }
        public void setTransferFinsourceMDMKey(String transferFinsourceMDMKey) { this.transferFinsourceMDMKey = transferFinsourceMDMKey; }
        public String getTransferFinsourceName() { return transferFinsourceName; }
        public void setTransferFinsourceName(String transferFinsourceName) { this.transferFinsourceName = transferFinsourceName; }
        public String getTransferSourcedocNum() { return transferSourcedocNum; }
        public void setTransferSourcedocNum(String transferSourcedocNum) { this.transferSourcedocNum = transferSourcedocNum; }
        public Timestamp getTransferSourcedocDate() { return transferSourcedocDate; }
        public void setTransferSourcedocDate(Timestamp transferSourcedocDate) { this.transferSourcedocDate = transferSourcedocDate; }
        public String getIdCpz() { return idCpz; }
        public void setIdCpz(String idCpz) { this.idCpz = idCpz; }
        public String getGoodsName() { return goodsName; }
        public void setGoodsName(String goodsName) { this.goodsName = goodsName; }
        public Integer getGoodsQnty() { return goodsQnty; }
        public void setGoodsQnty(Integer goodsQnty) { this.goodsQnty = goodsQnty; }
        public String getGoodsSn() { return goodsSn; }
        public void setGoodsSn(String goodsSn) { this.goodsSn = goodsSn; }
        public Timestamp getGoodsExpdate() { return goodsExpdate; }
        public void setGoodsExpdate(Timestamp goodsExpdate) { this.goodsExpdate = goodsExpdate; }
        public Double getGoodsPrice() { return goodsPrice; }
        public void setGoodsPrice(Double goodsPrice) { this.goodsPrice = goodsPrice; }
        public String getGoodsMesunit() { return goodsMesunit; }
        public void setGoodsMesunit(String goodsMesunit) { this.goodsMesunit = goodsMesunit; }
        public String getGoodsBarcode() { return goodsBarcode; }
        public void setGoodsBarcode(String goodsBarcode) { this.goodsBarcode = goodsBarcode; }
        public Double getGoodsSum() { return goodsSum; }
        public void setGoodsSum(Double goodsSum) { this.goodsSum = goodsSum; }
        public Double getGoodsSumnds() { return goodsSumnds; }
        public void setGoodsSumnds(Double goodsSumnds) { this.goodsSumnds = goodsSumnds; }
        public String getGoodsNdspercent() { return goodsNdspercent; }
        public void setGoodsNdspercent(String goodsNdspercent) { this.goodsNdspercent = goodsNdspercent; }
        public Double getGoodsProducerprice() { return goodsProducerprice; }
        public void setGoodsProducerprice(Double goodsProducerprice) { this.goodsProducerprice = goodsProducerprice; }
        public Timestamp getLoadTimestamp() { return loadTimestamp; }
        public void setLoadTimestamp(Timestamp loadTimestamp) { this.loadTimestamp = loadTimestamp; }
    }
}

