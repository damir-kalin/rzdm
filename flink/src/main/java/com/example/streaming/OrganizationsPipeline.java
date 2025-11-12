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

public class OrganizationsPipeline {

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
                .setTopics("sys__nsi__esud_rzdm__organization__data")
                .setGroupId("flink-organizations-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Create data stream from Kafka
        DataStream<String> stream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Organizations Source")
                .name("Kafka Organizations Source")
                .uid("kafka-organizations-source");

        // Transform the JSON data with null handling
        // Используем FlatMap для обработки массива Items (может быть несколько элементов)
        DataStream<Organization> eventStream = stream.flatMap(new FlatMapFunction<String, Organization>() {
            @Override
            public void flatMap(String value, Collector<Organization> out) throws Exception {
                // Валидация и очистка входных данных
                if (value == null || value.trim().isEmpty()) {
                    System.err.println("Warning: Received null or empty message, skipping...");
                    return;
                }
                
                String trimmedValue = value.trim();
                org.json.JSONArray itemsToProcess = new org.json.JSONArray();
                
                try {
                    if (trimmedValue.startsWith("[")) {
                        // Это JSON массив - извлекаем Items
                        org.json.JSONArray jsonArray = new org.json.JSONArray(trimmedValue);
                        if (jsonArray.length() > 0) {
                            org.json.JSONObject firstElement = jsonArray.getJSONObject(0);
                            if (firstElement.has("Items")) {
                                itemsToProcess = firstElement.getJSONArray("Items");
                                System.out.println("Extracting " + itemsToProcess.length() + " items from Items array");
                            } else {
                                // Если нет Items, используем весь массив как элементы
                                itemsToProcess = jsonArray;
                            }
                        } else {
                            System.err.println("Warning: JSON array is empty");
                            return;
                        }
                    } else if (trimmedValue.startsWith("{")) {
                        // Это обычный JSON объект - создаем массив с одним элементом
                        JSONObject json = new JSONObject(trimmedValue);
                        itemsToProcess.put(json);
                    } else {
                        System.err.println("Warning: Invalid JSON format (doesn't start with '{' or '['): " + 
                            (trimmedValue.length() > 100 ? trimmedValue.substring(0, 100) + "..." : trimmedValue));
                        return;
                    }
                } catch (org.json.JSONException e) {
                    System.err.println("Error parsing JSON: " + e.getMessage());
                    System.err.println("Problematic message (first 200 chars): " + 
                        (trimmedValue.length() > 200 ? trimmedValue.substring(0, 200) + "..." : trimmedValue));
                    return; // Пропускаем некорректные сообщения
                }
                
                // Обрабатываем каждый элемент из массива
                for (int i = 0; i < itemsToProcess.length(); i++) {
                    JSONObject json = itemsToProcess.getJSONObject(i);
                    Organization org = new Organization();
                // Основные поля
                org.setGUID(safeParseString(json, "GUID"));
                org.setUID(safeParseString(json, "UID"));
                org.setUIDSearchString(safeParseString(json, "UIDSearchString"));
                org.setINN(safeParseString(json, "INN"));
                org.setKPP(safeParseString(json, "KPP"));
                org.setOGRN(safeParseString(json, "OGRN"));
                org.setPolnoeNaimenovanie(safeParseString(json, "PolnoeNaimenovanie"));
                org.setNaimenovanieSokrashchennoe(safeParseString(json, "NaimenovanieSokrashchennoe"));
                org.setNaimenovanieMezhdunarodnoe(safeParseString(json, "NaimenovanieMezhdunarodnoe"));
                org.setNaimenovanieInostrOrganizatsii(safeParseString(json, "NaimenovanieInostrOrganizatsii"));
                org.setKodOKVED(safeParseString(json, "KodOKVED"));
                org.setKodOKVED2(safeParseString(json, "KodOKVED2"));
                org.setKodOKOPF(safeParseString(json, "KodOKOPF"));
                org.setKodOKFS(safeParseString(json, "KodOKFS"));
                org.setKodPoOKPO(safeParseString(json, "KodPoOKPO"));
                org.setKodPoOKATO(safeParseString(json, "KodPoOKATO"));
                org.setStranaRegistratsii(safeParseString(json, "StranaRegistratsii"));
                org.setStranaRegistratsiiInostrannoyOrganizatsii(safeParseString(json, "StranaRegistratsiiInostrannoyOrganizatsii"));
                org.setStranaPostoyannogoMestonakhozhdeniya(safeParseString(json, "StranaPostoyannogoMestonakhozhdeniya"));
                org.setDataRegistratsii(safeParseString(json, "DataRegistratsii"));
                org.setMomentOfTime(parseDateTime(json, "MomentOfTime"));
                
                // rc_ поля
                org.setRcAudit(safeParseString(json, "rc_Audit"));
                org.setRcAuditor(safeParseString(json, "rc_Auditor"));
                org.setRcVidOrganizatsiiPoUmolchaniyu(safeParseString(json, "rc_VidOrganizatsiiPoUmolchaniyu"));
                org.setRcGlavnyyBukhgalterFIO(safeParseString(json, "rc_GlavnyyBukhgalterFIO"));
                org.setRcGruppaKontragenta(safeParseString(json, "rc_GruppaKontragenta"));
                org.setRcDataPrekrashcheniyaDeyatelnosti(safeParseString(json, "rc_DataPrekrashcheniyaDeyatelnosti"));
                org.setRcDivizion(safeParseString(json, "rc_Divizion"));
                org.setRcZakupkaPoFZ223(safeParseBoolean(json, "rc_ZakupkaPoFZ223"));
                org.setRcMakroregion(safeParseString(json, "rc_Makroregion"));
                org.setRcNaimenovanieBE(safeParseString(json, "rc_NaimenovanieBE"));
                org.setRcOsnovnoyBankovskiySchet(safeParseString(json, "rc_OsnovnoyBankovskiySchet"));
                org.setRcRegionRossii(safeParseString(json, "rc_RegionRossii"));
                org.setRcRukovoditelOrganizatsiiFIO(safeParseString(json, "rc_RukovoditelOrganizatsiiFIO"));
                org.setRcEtoBank(safeParseBoolean(json, "rc_EtoBank"));
                org.setRcEtoIndividualnyyPredprinimatel(safeParseBoolean(json, "rc_EtoIndividualnyyPredprinimatel"));
                org.setRcEtoStrakhovayaKompaniya(safeParseBoolean(json, "rc_EtoStrakhovayaKompaniya"));
                
                // Дополнительные поля
                org.setIndividualnyyPredprinimatel(safeParseString(json, "IndividualnyyPredprinimatel"));
                org.setKontragent(safeParseString(json, "Kontragent"));
                org.setKrupneyshiyNalogoplatelshchik(safeParseBoolean(json, "KrupneyshiyNalogoplatelshchik"));
                org.setGolovnayaOrganizatsiya(safeParseString(json, "GolovnayaOrganizatsiya"));
                org.setObosoblennoePodrazdelenie(safeParseBoolean(json, "ObosoblennoePodrazdelenie"));
                org.setYuridicheskoeFizicheskoeLitso(safeParseString(json, "YuridicheskoeFizicheskoeLitso"));
                org.setYurFizLitso(safeParseString(json, "YurFizLitso"));
                org.setKodVStraneRegistratsii(safeParseString(json, "KodVStraneRegistratsii"));
                org.setKodNalogovogoOrgana(safeParseString(json, "KodNalogovogoOrgana"));
                org.setKodOrganaPFR(safeParseString(json, "KodOrganaPFR"));
                org.setRegistratsionnyyNomerPFR(safeParseString(json, "RegistratsionnyyNomerPFR"));
                org.setRegistratsionnyyNomerTFOMS(safeParseString(json, "RegistratsionnyyNomerTFOMS"));
                org.setRegistratsionnyyNomerFSS(safeParseString(json, "RegistratsionnyyNomerFSS"));
                
                // Timestamp загрузки
                org.setLoadTimestamp(Timestamp.valueOf(LocalDateTime.now()));
                
                    // Проверка обязательного поля GUID
                    if (org.getGUID() == null || org.getGUID().trim().isEmpty()) {
                        System.err.println("Warning: Message without GUID, skipping...");
                        continue; // Пропускаем этот элемент
                    }
                    
                    out.collect(org);
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
            
            private Boolean safeParseBoolean(JSONObject json, String key) {
                if (!json.has(key) || json.isNull(key)) return null;
                try {
                    return json.getBoolean(key);
                } catch (Exception e) {
                    return null;
                }
            }
            
            private Timestamp parseDateTime(JSONObject json, String key) {
                String value = safeParseString(json, key);
                if (value == null || value.isEmpty() || value.equals("0001-01-01T00:00:00")) {
                    return null;
                }
                try {
                    // Формат: "2025-10-28T09:14:30"
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
                    LocalDateTime dateTime = LocalDateTime.parse(value, formatter);
                    return Timestamp.valueOf(dateTime);
                } catch (Exception e) {
                    return null;
                }
            }
        }).name("Parse organization payload")
          .uid("parse-organizations");

        // Create a JDBC sink to StarRocks
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

        JdbcStatementBuilder<Organization> statementBuilder = (statement, org) -> {
            int index = 1;
            setNullableString(statement, index++, org.getGUID());
            setNullableString(statement, index++, org.getUID());
            setNullableString(statement, index++, org.getUIDSearchString());
            setNullableString(statement, index++, org.getINN());
            setNullableString(statement, index++, org.getKPP());
            setNullableString(statement, index++, org.getOGRN());
            setNullableString(statement, index++, org.getPolnoeNaimenovanie());
            setNullableString(statement, index++, org.getNaimenovanieSokrashchennoe());
            setNullableString(statement, index++, org.getNaimenovanieMezhdunarodnoe());
            setNullableString(statement, index++, org.getNaimenovanieInostrOrganizatsii());
            setNullableString(statement, index++, org.getKodOKVED());
            setNullableString(statement, index++, org.getKodOKVED2());
            setNullableString(statement, index++, org.getKodOKOPF());
            setNullableString(statement, index++, org.getKodOKFS());
            setNullableString(statement, index++, org.getKodPoOKPO());
            setNullableString(statement, index++, org.getKodPoOKATO());
            setNullableString(statement, index++, org.getStranaRegistratsii());
            setNullableString(statement, index++, org.getStranaRegistratsiiInostrannoyOrganizatsii());
            setNullableString(statement, index++, org.getStranaPostoyannogoMestonakhozhdeniya());
            setNullableString(statement, index++, org.getDataRegistratsii());
            setNullableTimestamp(statement, index++, org.getMomentOfTime());
            setNullableString(statement, index++, org.getRcAudit());
            setNullableString(statement, index++, org.getRcAuditor());
            setNullableString(statement, index++, org.getRcVidOrganizatsiiPoUmolchaniyu());
            setNullableString(statement, index++, org.getRcGlavnyyBukhgalterFIO());
            setNullableString(statement, index++, org.getRcGruppaKontragenta());
            setNullableString(statement, index++, org.getRcDataPrekrashcheniyaDeyatelnosti());
            setNullableString(statement, index++, org.getRcDivizion());
            setNullableBoolean(statement, index++, org.getRcZakupkaPoFZ223());
            setNullableString(statement, index++, org.getRcMakroregion());
            setNullableString(statement, index++, org.getRcNaimenovanieBE());
            setNullableString(statement, index++, org.getRcOsnovnoyBankovskiySchet());
            setNullableString(statement, index++, org.getRcRegionRossii());
            setNullableString(statement, index++, org.getRcRukovoditelOrganizatsiiFIO());
            setNullableBoolean(statement, index++, org.getRcEtoBank());
            setNullableBoolean(statement, index++, org.getRcEtoIndividualnyyPredprinimatel());
            setNullableBoolean(statement, index++, org.getRcEtoStrakhovayaKompaniya());
            setNullableString(statement, index++, org.getIndividualnyyPredprinimatel());
            setNullableString(statement, index++, org.getKontragent());
            setNullableBoolean(statement, index++, org.getKrupneyshiyNalogoplatelshchik());
            setNullableString(statement, index++, org.getGolovnayaOrganizatsiya());
            setNullableBoolean(statement, index++, org.getObosoblennoePodrazdelenie());
            setNullableString(statement, index++, org.getYuridicheskoeFizicheskoeLitso());
            setNullableString(statement, index++, org.getYurFizLitso());
            setNullableString(statement, index++, org.getKodVStraneRegistratsii());
            setNullableString(statement, index++, org.getKodNalogovogoOrgana());
            setNullableString(statement, index++, org.getKodOrganaPFR());
            setNullableString(statement, index++, org.getRegistratsionnyyNomerPFR());
            setNullableString(statement, index++, org.getRegistratsionnyyNomerTFOMS());
            setNullableString(statement, index++, org.getRegistratsionnyyNomerFSS());
            setNullableTimestamp(statement, index++, org.getLoadTimestamp());
        };

        final int parameterCount = 51;

        String insertSql = "INSERT INTO iceberg.rzdm_test.stg_organizations (" +
                    "GUID, UID, UIDSearchString, INN, KPP, OGRN, " +
                    "PolnoeNaimenovanie, NaimenovanieSokrashchennoe, NaimenovanieMezhdunarodnoe, " +
                    "NaimenovanieInostrOrganizatsii, KodOKVED, KodOKVED2, KodOKOPF, KodOKFS, " +
                    "KodPoOKPO, KodPoOKATO, StranaRegistratsii, StranaRegistratsiiInostrannoyOrganizatsii, " +
                    "StranaPostoyannogoMestonakhozhdeniya, DataRegistratsii, MomentOfTime, " +
                    "rc_Audit, rc_Auditor, rc_VidOrganizatsiiPoUmolchaniyu, rc_GlavnyyBukhgalterFIO, " +
                    "rc_GruppaKontragenta, rc_DataPrekrashcheniyaDeyatelnosti, rc_Divizion, " +
                    "rc_ZakupkaPoFZ223, rc_Makroregion, rc_NaimenovanieBE, rc_OsnovnoyBankovskiySchet, " +
                    "rc_RegionRossii, rc_RukovoditelOrganizatsiiFIO, rc_EtoBank, " +
                    "rc_EtoIndividualnyyPredprinimatel, rc_EtoStrakhovayaKompaniya, " +
                    "IndividualnyyPredprinimatel, Kontragent, KrupneyshiyNalogoplatelshchik, " +
                    "GolovnayaOrganizatsiya, ObosoblennoePodrazdelenie, YuridicheskoeFizicheskoeLitso, " +
                    "YurFizLitso, KodVStraneRegistratsii, KodNalogovogoOrgana, KodOrganaPFR, " +
                    "RegistratsionnyyNomerPFR, RegistratsionnyyNomerTFOMS, RegistratsionnyyNomerFSS, " +
                    "load_timestamp" +
                ") VALUES (" +
                    String.join(", ", Collections.nCopies(parameterCount, "?")) +
                ")";

        // Фильтруем null значения перед отправкой в sink
        DataStream<Organization> filteredStream = eventStream
                .filter(Objects::nonNull)
                .name("Filter null organizations")
                .uid("filter-organizations");
        
        // Add the StarRocks sink
        JdbcSink<Organization> starRocksSink = JdbcSink.<Organization>builder()
                .withQueryStatement(insertSql, statementBuilder)
                .withExecutionOptions(executionOptions)
                .buildAtLeastOnce(connectionOptions);

        filteredStream
                .sinkTo(starRocksSink)
                .name("StarRocks JDBC Sink")
                .uid("organizations-jdbc-sink")
                .setParallelism(1);

        // Execute the streaming pipeline
        env.execute("Organizations Streaming Pipeline");
    }

    private static StreamExecutionEnvironment createExecutionEnvironment() {
        Configuration config = new Configuration();
        String restAddress = System.getenv().getOrDefault("FLINK_REST_ADDRESS", "flink-1762357787-jobmanager");
        int restPort = Integer.parseInt(System.getenv().getOrDefault("FLINK_REST_PORT", "8081"));
        config.set(RestOptions.ADDRESS, restAddress);
        config.set(RestOptions.PORT, restPort);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(config, OrganizationsPipeline.class.getClassLoader());
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

    private static void setNullableBoolean(java.sql.PreparedStatement statement, int index, Boolean value) throws java.sql.SQLException {
        if (value == null) {
            statement.setNull(index, Types.BOOLEAN);
        } else {
            statement.setBoolean(index, value);
        }
    }

    private static void setNullableTimestamp(java.sql.PreparedStatement statement, int index, Timestamp value) throws java.sql.SQLException {
        if (value == null) {
            statement.setNull(index, Types.TIMESTAMP);
        } else {
            statement.setTimestamp(index, value);
        }
    }

    // Organization POJO class
    public static class Organization {
        private String GUID;
        private String UID;
        private String UIDSearchString;
        private String INN;
        private String KPP;
        private String OGRN;
        private String PolnoeNaimenovanie;
        private String NaimenovanieSokrashchennoe;
        private String NaimenovanieMezhdunarodnoe;
        private String NaimenovanieInostrOrganizatsii;
        private String KodOKVED;
        private String KodOKVED2;
        private String KodOKOPF;
        private String KodOKFS;
        private String KodPoOKPO;
        private String KodPoOKATO;
        private String StranaRegistratsii;
        private String StranaRegistratsiiInostrannoyOrganizatsii;
        private String StranaPostoyannogoMestonakhozhdeniya;
        private String DataRegistratsii;
        private Timestamp MomentOfTime;
        private String rcAudit;
        private String rcAuditor;
        private String rcVidOrganizatsiiPoUmolchaniyu;
        private String rcGlavnyyBukhgalterFIO;
        private String rcGruppaKontragenta;
        private String rcDataPrekrashcheniyaDeyatelnosti;
        private String rcDivizion;
        private Boolean rcZakupkaPoFZ223;
        private String rcMakroregion;
        private String rcNaimenovanieBE;
        private String rcOsnovnoyBankovskiySchet;
        private String rcRegionRossii;
        private String rcRukovoditelOrganizatsiiFIO;
        private Boolean rcEtoBank;
        private Boolean rcEtoIndividualnyyPredprinimatel;
        private Boolean rcEtoStrakhovayaKompaniya;
        private String IndividualnyyPredprinimatel;
        private String Kontragent;
        private Boolean KrupneyshiyNalogoplatelshchik;
        private String GolovnayaOrganizatsiya;
        private Boolean ObosoblennoePodrazdelenie;
        private String YuridicheskoeFizicheskoeLitso;
        private String YurFizLitso;
        private String KodVStraneRegistratsii;
        private String KodNalogovogoOrgana;
        private String KodOrganaPFR;
        private String RegistratsionnyyNomerPFR;
        private String RegistratsionnyyNomerTFOMS;
        private String RegistratsionnyyNomerFSS;
        private Timestamp LoadTimestamp;

        // Getters and setters
        public String getGUID() { return GUID; }
        public void setGUID(String GUID) { this.GUID = GUID; }
        public String getUID() { return UID; }
        public void setUID(String UID) { this.UID = UID; }
        public String getUIDSearchString() { return UIDSearchString; }
        public void setUIDSearchString(String UIDSearchString) { this.UIDSearchString = UIDSearchString; }
        public String getINN() { return INN; }
        public void setINN(String INN) { this.INN = INN; }
        public String getKPP() { return KPP; }
        public void setKPP(String KPP) { this.KPP = KPP; }
        public String getOGRN() { return OGRN; }
        public void setOGRN(String OGRN) { this.OGRN = OGRN; }
        public String getPolnoeNaimenovanie() { return PolnoeNaimenovanie; }
        public void setPolnoeNaimenovanie(String PolnoeNaimenovanie) { this.PolnoeNaimenovanie = PolnoeNaimenovanie; }
        public String getNaimenovanieSokrashchennoe() { return NaimenovanieSokrashchennoe; }
        public void setNaimenovanieSokrashchennoe(String NaimenovanieSokrashchennoe) { this.NaimenovanieSokrashchennoe = NaimenovanieSokrashchennoe; }
        public String getNaimenovanieMezhdunarodnoe() { return NaimenovanieMezhdunarodnoe; }
        public void setNaimenovanieMezhdunarodnoe(String NaimenovanieMezhdunarodnoe) { this.NaimenovanieMezhdunarodnoe = NaimenovanieMezhdunarodnoe; }
        public String getNaimenovanieInostrOrganizatsii() { return NaimenovanieInostrOrganizatsii; }
        public void setNaimenovanieInostrOrganizatsii(String NaimenovanieInostrOrganizatsii) { this.NaimenovanieInostrOrganizatsii = NaimenovanieInostrOrganizatsii; }
        public String getKodOKVED() { return KodOKVED; }
        public void setKodOKVED(String KodOKVED) { this.KodOKVED = KodOKVED; }
        public String getKodOKVED2() { return KodOKVED2; }
        public void setKodOKVED2(String KodOKVED2) { this.KodOKVED2 = KodOKVED2; }
        public String getKodOKOPF() { return KodOKOPF; }
        public void setKodOKOPF(String KodOKOPF) { this.KodOKOPF = KodOKOPF; }
        public String getKodOKFS() { return KodOKFS; }
        public void setKodOKFS(String KodOKFS) { this.KodOKFS = KodOKFS; }
        public String getKodPoOKPO() { return KodPoOKPO; }
        public void setKodPoOKPO(String KodPoOKPO) { this.KodPoOKPO = KodPoOKPO; }
        public String getKodPoOKATO() { return KodPoOKATO; }
        public void setKodPoOKATO(String KodPoOKATO) { this.KodPoOKATO = KodPoOKATO; }
        public String getStranaRegistratsii() { return StranaRegistratsii; }
        public void setStranaRegistratsii(String StranaRegistratsii) { this.StranaRegistratsii = StranaRegistratsii; }
        public String getStranaRegistratsiiInostrannoyOrganizatsii() { return StranaRegistratsiiInostrannoyOrganizatsii; }
        public void setStranaRegistratsiiInostrannoyOrganizatsii(String StranaRegistratsiiInostrannoyOrganizatsii) { this.StranaRegistratsiiInostrannoyOrganizatsii = StranaRegistratsiiInostrannoyOrganizatsii; }
        public String getStranaPostoyannogoMestonakhozhdeniya() { return StranaPostoyannogoMestonakhozhdeniya; }
        public void setStranaPostoyannogoMestonakhozhdeniya(String StranaPostoyannogoMestonakhozhdeniya) { this.StranaPostoyannogoMestonakhozhdeniya = StranaPostoyannogoMestonakhozhdeniya; }
        public String getDataRegistratsii() { return DataRegistratsii; }
        public void setDataRegistratsii(String DataRegistratsii) { this.DataRegistratsii = DataRegistratsii; }
        public Timestamp getMomentOfTime() { return MomentOfTime; }
        public void setMomentOfTime(Timestamp MomentOfTime) { this.MomentOfTime = MomentOfTime; }
        public String getRcAudit() { return rcAudit; }
        public void setRcAudit(String rcAudit) { this.rcAudit = rcAudit; }
        public String getRcAuditor() { return rcAuditor; }
        public void setRcAuditor(String rcAuditor) { this.rcAuditor = rcAuditor; }
        public String getRcVidOrganizatsiiPoUmolchaniyu() { return rcVidOrganizatsiiPoUmolchaniyu; }
        public void setRcVidOrganizatsiiPoUmolchaniyu(String rcVidOrganizatsiiPoUmolchaniyu) { this.rcVidOrganizatsiiPoUmolchaniyu = rcVidOrganizatsiiPoUmolchaniyu; }
        public String getRcGlavnyyBukhgalterFIO() { return rcGlavnyyBukhgalterFIO; }
        public void setRcGlavnyyBukhgalterFIO(String rcGlavnyyBukhgalterFIO) { this.rcGlavnyyBukhgalterFIO = rcGlavnyyBukhgalterFIO; }
        public String getRcGruppaKontragenta() { return rcGruppaKontragenta; }
        public void setRcGruppaKontragenta(String rcGruppaKontragenta) { this.rcGruppaKontragenta = rcGruppaKontragenta; }
        public String getRcDataPrekrashcheniyaDeyatelnosti() { return rcDataPrekrashcheniyaDeyatelnosti; }
        public void setRcDataPrekrashcheniyaDeyatelnosti(String rcDataPrekrashcheniyaDeyatelnosti) { this.rcDataPrekrashcheniyaDeyatelnosti = rcDataPrekrashcheniyaDeyatelnosti; }
        public String getRcDivizion() { return rcDivizion; }
        public void setRcDivizion(String rcDivizion) { this.rcDivizion = rcDivizion; }
        public Boolean getRcZakupkaPoFZ223() { return rcZakupkaPoFZ223; }
        public void setRcZakupkaPoFZ223(Boolean rcZakupkaPoFZ223) { this.rcZakupkaPoFZ223 = rcZakupkaPoFZ223; }
        public String getRcMakroregion() { return rcMakroregion; }
        public void setRcMakroregion(String rcMakroregion) { this.rcMakroregion = rcMakroregion; }
        public String getRcNaimenovanieBE() { return rcNaimenovanieBE; }
        public void setRcNaimenovanieBE(String rcNaimenovanieBE) { this.rcNaimenovanieBE = rcNaimenovanieBE; }
        public String getRcOsnovnoyBankovskiySchet() { return rcOsnovnoyBankovskiySchet; }
        public void setRcOsnovnoyBankovskiySchet(String rcOsnovnoyBankovskiySchet) { this.rcOsnovnoyBankovskiySchet = rcOsnovnoyBankovskiySchet; }
        public String getRcRegionRossii() { return rcRegionRossii; }
        public void setRcRegionRossii(String rcRegionRossii) { this.rcRegionRossii = rcRegionRossii; }
        public String getRcRukovoditelOrganizatsiiFIO() { return rcRukovoditelOrganizatsiiFIO; }
        public void setRcRukovoditelOrganizatsiiFIO(String rcRukovoditelOrganizatsiiFIO) { this.rcRukovoditelOrganizatsiiFIO = rcRukovoditelOrganizatsiiFIO; }
        public Boolean getRcEtoBank() { return rcEtoBank; }
        public void setRcEtoBank(Boolean rcEtoBank) { this.rcEtoBank = rcEtoBank; }
        public Boolean getRcEtoIndividualnyyPredprinimatel() { return rcEtoIndividualnyyPredprinimatel; }
        public void setRcEtoIndividualnyyPredprinimatel(Boolean rcEtoIndividualnyyPredprinimatel) { this.rcEtoIndividualnyyPredprinimatel = rcEtoIndividualnyyPredprinimatel; }
        public Boolean getRcEtoStrakhovayaKompaniya() { return rcEtoStrakhovayaKompaniya; }
        public void setRcEtoStrakhovayaKompaniya(Boolean rcEtoStrakhovayaKompaniya) { this.rcEtoStrakhovayaKompaniya = rcEtoStrakhovayaKompaniya; }
        public String getIndividualnyyPredprinimatel() { return IndividualnyyPredprinimatel; }
        public void setIndividualnyyPredprinimatel(String IndividualnyyPredprinimatel) { this.IndividualnyyPredprinimatel = IndividualnyyPredprinimatel; }
        public String getKontragent() { return Kontragent; }
        public void setKontragent(String Kontragent) { this.Kontragent = Kontragent; }
        public Boolean getKrupneyshiyNalogoplatelshchik() { return KrupneyshiyNalogoplatelshchik; }
        public void setKrupneyshiyNalogoplatelshchik(Boolean KrupneyshiyNalogoplatelshchik) { this.KrupneyshiyNalogoplatelshchik = KrupneyshiyNalogoplatelshchik; }
        public String getGolovnayaOrganizatsiya() { return GolovnayaOrganizatsiya; }
        public void setGolovnayaOrganizatsiya(String GolovnayaOrganizatsiya) { this.GolovnayaOrganizatsiya = GolovnayaOrganizatsiya; }
        public Boolean getObosoblennoePodrazdelenie() { return ObosoblennoePodrazdelenie; }
        public void setObosoblennoePodrazdelenie(Boolean ObosoblennoePodrazdelenie) { this.ObosoblennoePodrazdelenie = ObosoblennoePodrazdelenie; }
        public String getYuridicheskoeFizicheskoeLitso() { return YuridicheskoeFizicheskoeLitso; }
        public void setYuridicheskoeFizicheskoeLitso(String YuridicheskoeFizicheskoeLitso) { this.YuridicheskoeFizicheskoeLitso = YuridicheskoeFizicheskoeLitso; }
        public String getYurFizLitso() { return YurFizLitso; }
        public void setYurFizLitso(String YurFizLitso) { this.YurFizLitso = YurFizLitso; }
        public String getKodVStraneRegistratsii() { return KodVStraneRegistratsii; }
        public void setKodVStraneRegistratsii(String KodVStraneRegistratsii) { this.KodVStraneRegistratsii = KodVStraneRegistratsii; }
        public String getKodNalogovogoOrgana() { return KodNalogovogoOrgana; }
        public void setKodNalogovogoOrgana(String KodNalogovogoOrgana) { this.KodNalogovogoOrgana = KodNalogovogoOrgana; }
        public String getKodOrganaPFR() { return KodOrganaPFR; }
        public void setKodOrganaPFR(String KodOrganaPFR) { this.KodOrganaPFR = KodOrganaPFR; }
        public String getRegistratsionnyyNomerPFR() { return RegistratsionnyyNomerPFR; }
        public void setRegistratsionnyyNomerPFR(String RegistratsionnyyNomerPFR) { this.RegistratsionnyyNomerPFR = RegistratsionnyyNomerPFR; }
        public String getRegistratsionnyyNomerTFOMS() { return RegistratsionnyyNomerTFOMS; }
        public void setRegistratsionnyyNomerTFOMS(String RegistratsionnyyNomerTFOMS) { this.RegistratsionnyyNomerTFOMS = RegistratsionnyyNomerTFOMS; }
        public String getRegistratsionnyyNomerFSS() { return RegistratsionnyyNomerFSS; }
        public void setRegistratsionnyyNomerFSS(String RegistratsionnyyNomerFSS) { this.RegistratsionnyyNomerFSS = RegistratsionnyyNomerFSS; }
        public Timestamp getLoadTimestamp() { return LoadTimestamp; }
        public void setLoadTimestamp(Timestamp LoadTimestamp) { this.LoadTimestamp = LoadTimestamp; }
    }
}

