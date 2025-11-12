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

public class ContractorsPipeline {

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
                .setTopics("sys__nsi__esud_rzdm__contractor__data")
                .setGroupId("flink-contractors-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Create data stream from Kafka
        DataStream<String> stream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Contractors Source")
                .name("Kafka Contractors Source")
                .uid("kafka-contractors-source");

        // Transform the JSON data with null handling
        // Используем FlatMap для обработки массива Items (может быть несколько элементов)
        DataStream<Contractor> eventStream = stream.flatMap(new FlatMapFunction<String, Contractor>() {
            @Override
            public void flatMap(String value, Collector<Contractor> out) throws Exception {
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
                    Contractor contractor = new Contractor();
                // Основные поля
                contractor.setGUID(safeParseString(json, "GUID"));
                contractor.setUID(safeParseString(json, "UID"));
                contractor.setUIDSearchString(safeParseString(json, "UIDSearchString"));
                contractor.setIdentifikator(safeParseString(json, "Identifikator"));
                contractor.setINN(safeParseString(json, "INN"));
                contractor.setKPP(safeParseString(json, "KPP"));
                contractor.setOGRN(safeParseString(json, "OGRN"));
                contractor.setPolnoeNaimenovanie(safeParseString(json, "PolnoeNaimenovanie"));
                contractor.setSokrashchennoeNaimenovanie(safeParseString(json, "SokrashchennoeNaimenovanie"));
                contractor.setMezhdunarodnoeNaimenovanie(safeParseString(json, "MezhdunarodnoeNaimenovanie"));
                contractor.setNalogovyyNomer(safeParseString(json, "NalogovyyNomer"));
                contractor.setRegistratsionnyyNomerNerezidenta(safeParseString(json, "RegistratsionnyyNomerNerezidenta"));
                contractor.setKodOKOPF(safeParseString(json, "KodOKOPF"));
                contractor.setKodPoOKPO(safeParseString(json, "KodPoOKPO"));
                contractor.setNaimenovanieOKOPF(safeParseString(json, "NaimenovanieOKOPF"));
                contractor.setOrganizatsionnoPravovayaForma(safeParseString(json, "OrganizatsionnoPravovayaForma"));
                contractor.setOrganizatsionnayaEdinitsa(safeParseString(json, "OrganizatsionnayaEdinitsa"));
                contractor.setStranaRegistratsii(safeParseString(json, "StranaRegistratsii"));
                contractor.setMomentOfTime(parseDateTime(json, "MomentOfTime"));
                
                // rc_ поля
                contractor.setRcAdresMezhdunarodnyy(safeParseString(json, "rc_AdresMezhdunarodnyy"));
                contractor.setRcGruppaKontragenta(safeParseString(json, "rc_GruppaKontragenta"));
                contractor.setRcDataPostanovkiNaUchetVNalogovoy(safeParseString(json, "rc_DataPostanovkiNaUchetVNalogovoy"));
                contractor.setRcDataRozhdeniya(safeParseString(json, "rc_DataRozhdeniya"));
                contractor.setRcDULNomer(safeParseString(json, "rc_DULNomer"));
                contractor.setRcDULSeriya(safeParseString(json, "rc_DULSeriya"));
                contractor.setRcImya(safeParseString(json, "rc_Imya"));
                contractor.setRcIndividualnyyPredprinimatel(safeParseBoolean(json, "rc_IndividualnyyPredprinimatel"));
                contractor.setRcKanalProdazh(safeParseString(json, "rc_KanalProdazh"));
                contractor.setRcKodOKVED(safeParseString(json, "rc_KodOKVED"));
                contractor.setRcKodOKVED2(safeParseString(json, "rc_KodOKVED2"));
                contractor.setRcOsnovnoyBankovskiySchet(safeParseString(json, "rc_OsnovnoyBankovskiySchet"));
                contractor.setRcOtchestvo(safeParseString(json, "rc_Otchestvo"));
                contractor.setRcPol(safeParseString(json, "rc_Pol"));
                contractor.setRcPriznakMeditsinskoyOrganizatsii(safeParseBoolean(json, "rc_PriznakMeditsinskoyOrganizatsii"));
                contractor.setRcSNILS(safeParseString(json, "rc_SNILS"));
                contractor.setRcFamiliya(safeParseString(json, "rc_Familiya"));
                contractor.setRcFIOIDolzhnostRukovoditelya(safeParseString(json, "rc_FIOIDolzhnostRukovoditelya"));
                contractor.setRcEtoBank(safeParseBoolean(json, "rc_EtoBank"));
                contractor.setRcEtoStrakhovayaKompaniya(safeParseBoolean(json, "rc_EtoStrakhovayaKompaniya"));
                contractor.setRcYuridicheskoeFizicheskoeLitso(safeParseString(json, "rc_YuridicheskoeFizicheskoeLitso"));
                contractor.setRcSvidetelstvoDataVydachi(safeParseString(json, "rc_SvidetelstvoDataVydachi"));
                contractor.setRcSvidetelstvoSeriyaNomer(safeParseString(json, "rc_SvidetelstvoSeriyaNomer"));
                
                // Дополнительные поля
                contractor.setGolovnoyKontragent(safeParseString(json, "GolovnoyKontragent"));
                contractor.setKommentariy(safeParseString(json, "Kommentariy"));
                contractor.setOKDP(safeParseString(json, "OKDP"));
                contractor.setOpisanie(safeParseString(json, "Opisanie"));
                contractor.setPartner(safeParseString(json, "Partner"));
                contractor.setPriznakIspolzovaniya(safeParseString(json, "PriznakIspolzovaniya"));
                contractor.setNerezident(safeParseBoolean(json, "Nerezident"));
                contractor.setObosoblennoePodrazdelenie(safeParseBoolean(json, "ObosoblennoePodrazdelenie"));
                contractor.setYurFizLitso(safeParseString(json, "YurFizLitso"));
                contractor.setOsnovnoyBankovskiySchet(safeParseString(json, "OsnovnoyBankovskiySchet"));
                
                // Timestamp загрузки
                contractor.setLoadTimestamp(Timestamp.valueOf(LocalDateTime.now()));
                
                    // Проверка обязательного поля GUID
                    if (contractor.getGUID() == null || contractor.getGUID().trim().isEmpty()) {
                        System.err.println("Warning: Message without GUID, skipping...");
                        continue; // Пропускаем этот элемент
                    }
                    
                    out.collect(contractor);
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
                    // Формат: "2025-10-28T09:16:41"
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
                    LocalDateTime dateTime = LocalDateTime.parse(value, formatter);
                    return Timestamp.valueOf(dateTime);
                } catch (Exception e) {
                    return null;
                }
            }
        }).name("Parse contractor payload")
          .uid("parse-contractors");

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

        JdbcStatementBuilder<Contractor> statementBuilder = (statement, contractor) -> {
            int index = 1;
            setNullableString(statement, index++, contractor.getGUID());
            setNullableString(statement, index++, contractor.getUID());
            setNullableString(statement, index++, contractor.getUIDSearchString());
            setNullableString(statement, index++, contractor.getIdentifikator());
            setNullableString(statement, index++, contractor.getINN());
            setNullableString(statement, index++, contractor.getKPP());
            setNullableString(statement, index++, contractor.getOGRN());
            setNullableString(statement, index++, contractor.getPolnoeNaimenovanie());
            setNullableString(statement, index++, contractor.getSokrashchennoeNaimenovanie());
            setNullableString(statement, index++, contractor.getMezhdunarodnoeNaimenovanie());
            setNullableString(statement, index++, contractor.getNalogovyyNomer());
            setNullableString(statement, index++, contractor.getRegistratsionnyyNomerNerezidenta());
            setNullableString(statement, index++, contractor.getKodOKOPF());
            setNullableString(statement, index++, contractor.getKodPoOKPO());
            setNullableString(statement, index++, contractor.getNaimenovanieOKOPF());
            setNullableString(statement, index++, contractor.getOrganizatsionnoPravovayaForma());
            setNullableString(statement, index++, contractor.getOrganizatsionnayaEdinitsa());
            setNullableString(statement, index++, contractor.getStranaRegistratsii());
            setNullableTimestamp(statement, index++, contractor.getMomentOfTime());
            setNullableString(statement, index++, contractor.getRcAdresMezhdunarodnyy());
            setNullableString(statement, index++, contractor.getRcGruppaKontragenta());
            setNullableString(statement, index++, contractor.getRcDataPostanovkiNaUchetVNalogovoy());
            setNullableString(statement, index++, contractor.getRcDataRozhdeniya());
            setNullableString(statement, index++, contractor.getRcDULNomer());
            setNullableString(statement, index++, contractor.getRcDULSeriya());
            setNullableString(statement, index++, contractor.getRcImya());
            setNullableBoolean(statement, index++, contractor.getRcIndividualnyyPredprinimatel());
            setNullableString(statement, index++, contractor.getRcKanalProdazh());
            setNullableString(statement, index++, contractor.getRcKodOKVED());
            setNullableString(statement, index++, contractor.getRcKodOKVED2());
            setNullableString(statement, index++, contractor.getRcOsnovnoyBankovskiySchet());
            setNullableString(statement, index++, contractor.getRcOtchestvo());
            setNullableString(statement, index++, contractor.getRcPol());
            setNullableBoolean(statement, index++, contractor.getRcPriznakMeditsinskoyOrganizatsii());
            setNullableString(statement, index++, contractor.getRcSNILS());
            setNullableString(statement, index++, contractor.getRcFamiliya());
            setNullableString(statement, index++, contractor.getRcFIOIDolzhnostRukovoditelya());
            setNullableBoolean(statement, index++, contractor.getRcEtoBank());
            setNullableBoolean(statement, index++, contractor.getRcEtoStrakhovayaKompaniya());
            setNullableString(statement, index++, contractor.getRcYuridicheskoeFizicheskoeLitso());
            setNullableString(statement, index++, contractor.getRcSvidetelstvoDataVydachi());
            setNullableString(statement, index++, contractor.getRcSvidetelstvoSeriyaNomer());
            setNullableString(statement, index++, contractor.getGolovnoyKontragent());
            setNullableString(statement, index++, contractor.getKommentariy());
            setNullableString(statement, index++, contractor.getOKDP());
            setNullableString(statement, index++, contractor.getOpisanie());
            setNullableString(statement, index++, contractor.getPartner());
            setNullableString(statement, index++, contractor.getPriznakIspolzovaniya());
            setNullableBoolean(statement, index++, contractor.getNerezident());
            setNullableBoolean(statement, index++, contractor.getObosoblennoePodrazdelenie());
            setNullableString(statement, index++, contractor.getYurFizLitso());
            setNullableString(statement, index++, contractor.getOsnovnoyBankovskiySchet());
            setNullableTimestamp(statement, index++, contractor.getLoadTimestamp());
        };

        final int parameterCount = 53;

        String insertSql = "INSERT INTO iceberg.rzdm_test.stg_contractors (" +
                    "GUID, UID, UIDSearchString, Identifikator, INN, KPP, OGRN, " +
                    "PolnoeNaimenovanie, SokrashchennoeNaimenovanie, MezhdunarodnoeNaimenovanie, " +
                    "NalogovyyNomer, RegistratsionnyyNomerNerezidenta, KodOKOPF, KodPoOKPO, " +
                    "NaimenovanieOKOPF, OrganizatsionnoPravovayaForma, OrganizatsionnayaEdinitsa, " +
                    "StranaRegistratsii, MomentOfTime, " +
                    "rc_AdresMezhdunarodnyy, rc_GruppaKontragenta, rc_DataPostanovkiNaUchetVNalogovoy, " +
                    "rc_DataRozhdeniya, rc_DULNomer, rc_DULSeriya, rc_Imya, rc_IndividualnyyPredprinimatel, " +
                    "rc_KanalProdazh, rc_KodOKVED, rc_KodOKVED2, rc_OsnovnoyBankovskiySchet, " +
                    "rc_Otchestvo, rc_Pol, rc_PriznakMeditsinskoyOrganizatsii, rc_SNILS, " +
                    "rc_Familiya, rc_FIOIDolzhnostRukovoditelya, rc_EtoBank, rc_EtoStrakhovayaKompaniya, " +
                    "rc_YuridicheskoeFizicheskoeLitso, rc_SvidetelstvoDataVydachi, rc_SvidetelstvoSeriyaNomer, " +
                    "GolovnoyKontragent, Kommentariy, OKDP, Opisanie, Partner, PriznakIspolzovaniya, " +
                    "Nerezident, ObosoblennoePodrazdelenie, YurFizLitso, OsnovnoyBankovskiySchet, " +
                    "load_timestamp" +
                ") VALUES (" +
                    String.join(", ", Collections.nCopies(parameterCount, "?")) +
                ")";

        // Фильтруем null значения перед отправкой в sink
        DataStream<Contractor> filteredStream = eventStream
                .filter(Objects::nonNull)
                .name("Filter null contractors")
                .uid("filter-contractors");
        
        // Add the StarRocks sink
        JdbcSink<Contractor> starRocksSink = JdbcSink.<Contractor>builder()
                .withQueryStatement(insertSql, statementBuilder)
                .withExecutionOptions(executionOptions)
                .buildAtLeastOnce(connectionOptions);

        filteredStream
                .sinkTo(starRocksSink)
                .name("StarRocks JDBC Sink")
                .uid("contractors-jdbc-sink")
                .setParallelism(1);

        // Execute the streaming pipeline
        env.execute("Contractors Streaming Pipeline");
    }

    private static StreamExecutionEnvironment createExecutionEnvironment() {
        Configuration config = new Configuration();
        String restAddress = System.getenv().getOrDefault("FLINK_REST_ADDRESS", "flink-1762357787-jobmanager");
        int restPort = Integer.parseInt(System.getenv().getOrDefault("FLINK_REST_PORT", "8081"));
        config.set(RestOptions.ADDRESS, restAddress);
        config.set(RestOptions.PORT, restPort);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(config, ContractorsPipeline.class.getClassLoader());
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

    // Contractor POJO class
    public static class Contractor {
        private String GUID;
        private String UID;
        private String UIDSearchString;
        private String Identifikator;
        private String INN;
        private String KPP;
        private String OGRN;
        private String PolnoeNaimenovanie;
        private String SokrashchennoeNaimenovanie;
        private String MezhdunarodnoeNaimenovanie;
        private String NalogovyyNomer;
        private String RegistratsionnyyNomerNerezidenta;
        private String KodOKOPF;
        private String KodPoOKPO;
        private String NaimenovanieOKOPF;
        private String OrganizatsionnoPravovayaForma;
        private String OrganizatsionnayaEdinitsa;
        private String StranaRegistratsii;
        private Timestamp MomentOfTime;
        private String rcAdresMezhdunarodnyy;
        private String rcGruppaKontragenta;
        private String rcDataPostanovkiNaUchetVNalogovoy;
        private String rcDataRozhdeniya;
        private String rcDULNomer;
        private String rcDULSeriya;
        private String rcImya;
        private Boolean rcIndividualnyyPredprinimatel;
        private String rcKanalProdazh;
        private String rcKodOKVED;
        private String rcKodOKVED2;
        private String rcOsnovnoyBankovskiySchet;
        private String rcOtchestvo;
        private String rcPol;
        private Boolean rcPriznakMeditsinskoyOrganizatsii;
        private String rcSNILS;
        private String rcFamiliya;
        private String rcFIOIDolzhnostRukovoditelya;
        private Boolean rcEtoBank;
        private Boolean rcEtoStrakhovayaKompaniya;
        private String rcYuridicheskoeFizicheskoeLitso;
        private String rcSvidetelstvoDataVydachi;
        private String rcSvidetelstvoSeriyaNomer;
        private String GolovnoyKontragent;
        private String Kommentariy;
        private String OKDP;
        private String Opisanie;
        private String Partner;
        private String PriznakIspolzovaniya;
        private Boolean Nerezident;
        private Boolean ObosoblennoePodrazdelenie;
        private String YurFizLitso;
        private String OsnovnoyBankovskiySchet;
        private Timestamp LoadTimestamp;

        // Getters and setters
        public String getGUID() { return GUID; }
        public void setGUID(String GUID) { this.GUID = GUID; }
        public String getUID() { return UID; }
        public void setUID(String UID) { this.UID = UID; }
        public String getUIDSearchString() { return UIDSearchString; }
        public void setUIDSearchString(String UIDSearchString) { this.UIDSearchString = UIDSearchString; }
        public String getIdentifikator() { return Identifikator; }
        public void setIdentifikator(String Identifikator) { this.Identifikator = Identifikator; }
        public String getINN() { return INN; }
        public void setINN(String INN) { this.INN = INN; }
        public String getKPP() { return KPP; }
        public void setKPP(String KPP) { this.KPP = KPP; }
        public String getOGRN() { return OGRN; }
        public void setOGRN(String OGRN) { this.OGRN = OGRN; }
        public String getPolnoeNaimenovanie() { return PolnoeNaimenovanie; }
        public void setPolnoeNaimenovanie(String PolnoeNaimenovanie) { this.PolnoeNaimenovanie = PolnoeNaimenovanie; }
        public String getSokrashchennoeNaimenovanie() { return SokrashchennoeNaimenovanie; }
        public void setSokrashchennoeNaimenovanie(String SokrashchennoeNaimenovanie) { this.SokrashchennoeNaimenovanie = SokrashchennoeNaimenovanie; }
        public String getMezhdunarodnoeNaimenovanie() { return MezhdunarodnoeNaimenovanie; }
        public void setMezhdunarodnoeNaimenovanie(String MezhdunarodnoeNaimenovanie) { this.MezhdunarodnoeNaimenovanie = MezhdunarodnoeNaimenovanie; }
        public String getNalogovyyNomer() { return NalogovyyNomer; }
        public void setNalogovyyNomer(String NalogovyyNomer) { this.NalogovyyNomer = NalogovyyNomer; }
        public String getRegistratsionnyyNomerNerezidenta() { return RegistratsionnyyNomerNerezidenta; }
        public void setRegistratsionnyyNomerNerezidenta(String RegistratsionnyyNomerNerezidenta) { this.RegistratsionnyyNomerNerezidenta = RegistratsionnyyNomerNerezidenta; }
        public String getKodOKOPF() { return KodOKOPF; }
        public void setKodOKOPF(String KodOKOPF) { this.KodOKOPF = KodOKOPF; }
        public String getKodPoOKPO() { return KodPoOKPO; }
        public void setKodPoOKPO(String KodPoOKPO) { this.KodPoOKPO = KodPoOKPO; }
        public String getNaimenovanieOKOPF() { return NaimenovanieOKOPF; }
        public void setNaimenovanieOKOPF(String NaimenovanieOKOPF) { this.NaimenovanieOKOPF = NaimenovanieOKOPF; }
        public String getOrganizatsionnoPravovayaForma() { return OrganizatsionnoPravovayaForma; }
        public void setOrganizatsionnoPravovayaForma(String OrganizatsionnoPravovayaForma) { this.OrganizatsionnoPravovayaForma = OrganizatsionnoPravovayaForma; }
        public String getOrganizatsionnayaEdinitsa() { return OrganizatsionnayaEdinitsa; }
        public void setOrganizatsionnayaEdinitsa(String OrganizatsionnayaEdinitsa) { this.OrganizatsionnayaEdinitsa = OrganizatsionnayaEdinitsa; }
        public String getStranaRegistratsii() { return StranaRegistratsii; }
        public void setStranaRegistratsii(String StranaRegistratsii) { this.StranaRegistratsii = StranaRegistratsii; }
        public Timestamp getMomentOfTime() { return MomentOfTime; }
        public void setMomentOfTime(Timestamp MomentOfTime) { this.MomentOfTime = MomentOfTime; }
        public String getRcAdresMezhdunarodnyy() { return rcAdresMezhdunarodnyy; }
        public void setRcAdresMezhdunarodnyy(String rcAdresMezhdunarodnyy) { this.rcAdresMezhdunarodnyy = rcAdresMezhdunarodnyy; }
        public String getRcGruppaKontragenta() { return rcGruppaKontragenta; }
        public void setRcGruppaKontragenta(String rcGruppaKontragenta) { this.rcGruppaKontragenta = rcGruppaKontragenta; }
        public String getRcDataPostanovkiNaUchetVNalogovoy() { return rcDataPostanovkiNaUchetVNalogovoy; }
        public void setRcDataPostanovkiNaUchetVNalogovoy(String rcDataPostanovkiNaUchetVNalogovoy) { this.rcDataPostanovkiNaUchetVNalogovoy = rcDataPostanovkiNaUchetVNalogovoy; }
        public String getRcDataRozhdeniya() { return rcDataRozhdeniya; }
        public void setRcDataRozhdeniya(String rcDataRozhdeniya) { this.rcDataRozhdeniya = rcDataRozhdeniya; }
        public String getRcDULNomer() { return rcDULNomer; }
        public void setRcDULNomer(String rcDULNomer) { this.rcDULNomer = rcDULNomer; }
        public String getRcDULSeriya() { return rcDULSeriya; }
        public void setRcDULSeriya(String rcDULSeriya) { this.rcDULSeriya = rcDULSeriya; }
        public String getRcImya() { return rcImya; }
        public void setRcImya(String rcImya) { this.rcImya = rcImya; }
        public Boolean getRcIndividualnyyPredprinimatel() { return rcIndividualnyyPredprinimatel; }
        public void setRcIndividualnyyPredprinimatel(Boolean rcIndividualnyyPredprinimatel) { this.rcIndividualnyyPredprinimatel = rcIndividualnyyPredprinimatel; }
        public String getRcKanalProdazh() { return rcKanalProdazh; }
        public void setRcKanalProdazh(String rcKanalProdazh) { this.rcKanalProdazh = rcKanalProdazh; }
        public String getRcKodOKVED() { return rcKodOKVED; }
        public void setRcKodOKVED(String rcKodOKVED) { this.rcKodOKVED = rcKodOKVED; }
        public String getRcKodOKVED2() { return rcKodOKVED2; }
        public void setRcKodOKVED2(String rcKodOKVED2) { this.rcKodOKVED2 = rcKodOKVED2; }
        public String getRcOsnovnoyBankovskiySchet() { return rcOsnovnoyBankovskiySchet; }
        public void setRcOsnovnoyBankovskiySchet(String rcOsnovnoyBankovskiySchet) { this.rcOsnovnoyBankovskiySchet = rcOsnovnoyBankovskiySchet; }
        public String getRcOtchestvo() { return rcOtchestvo; }
        public void setRcOtchestvo(String rcOtchestvo) { this.rcOtchestvo = rcOtchestvo; }
        public String getRcPol() { return rcPol; }
        public void setRcPol(String rcPol) { this.rcPol = rcPol; }
        public Boolean getRcPriznakMeditsinskoyOrganizatsii() { return rcPriznakMeditsinskoyOrganizatsii; }
        public void setRcPriznakMeditsinskoyOrganizatsii(Boolean rcPriznakMeditsinskoyOrganizatsii) { this.rcPriznakMeditsinskoyOrganizatsii = rcPriznakMeditsinskoyOrganizatsii; }
        public String getRcSNILS() { return rcSNILS; }
        public void setRcSNILS(String rcSNILS) { this.rcSNILS = rcSNILS; }
        public String getRcFamiliya() { return rcFamiliya; }
        public void setRcFamiliya(String rcFamiliya) { this.rcFamiliya = rcFamiliya; }
        public String getRcFIOIDolzhnostRukovoditelya() { return rcFIOIDolzhnostRukovoditelya; }
        public void setRcFIOIDolzhnostRukovoditelya(String rcFIOIDolzhnostRukovoditelya) { this.rcFIOIDolzhnostRukovoditelya = rcFIOIDolzhnostRukovoditelya; }
        public Boolean getRcEtoBank() { return rcEtoBank; }
        public void setRcEtoBank(Boolean rcEtoBank) { this.rcEtoBank = rcEtoBank; }
        public Boolean getRcEtoStrakhovayaKompaniya() { return rcEtoStrakhovayaKompaniya; }
        public void setRcEtoStrakhovayaKompaniya(Boolean rcEtoStrakhovayaKompaniya) { this.rcEtoStrakhovayaKompaniya = rcEtoStrakhovayaKompaniya; }
        public String getRcYuridicheskoeFizicheskoeLitso() { return rcYuridicheskoeFizicheskoeLitso; }
        public void setRcYuridicheskoeFizicheskoeLitso(String rcYuridicheskoeFizicheskoeLitso) { this.rcYuridicheskoeFizicheskoeLitso = rcYuridicheskoeFizicheskoeLitso; }
        public String getRcSvidetelstvoDataVydachi() { return rcSvidetelstvoDataVydachi; }
        public void setRcSvidetelstvoDataVydachi(String rcSvidetelstvoDataVydachi) { this.rcSvidetelstvoDataVydachi = rcSvidetelstvoDataVydachi; }
        public String getRcSvidetelstvoSeriyaNomer() { return rcSvidetelstvoSeriyaNomer; }
        public void setRcSvidetelstvoSeriyaNomer(String rcSvidetelstvoSeriyaNomer) { this.rcSvidetelstvoSeriyaNomer = rcSvidetelstvoSeriyaNomer; }
        public String getGolovnoyKontragent() { return GolovnoyKontragent; }
        public void setGolovnoyKontragent(String GolovnoyKontragent) { this.GolovnoyKontragent = GolovnoyKontragent; }
        public String getKommentariy() { return Kommentariy; }
        public void setKommentariy(String Kommentariy) { this.Kommentariy = Kommentariy; }
        public String getOKDP() { return OKDP; }
        public void setOKDP(String OKDP) { this.OKDP = OKDP; }
        public String getOpisanie() { return Opisanie; }
        public void setOpisanie(String Opisanie) { this.Opisanie = Opisanie; }
        public String getPartner() { return Partner; }
        public void setPartner(String Partner) { this.Partner = Partner; }
        public String getPriznakIspolzovaniya() { return PriznakIspolzovaniya; }
        public void setPriznakIspolzovaniya(String PriznakIspolzovaniya) { this.PriznakIspolzovaniya = PriznakIspolzovaniya; }
        public Boolean getNerezident() { return Nerezident; }
        public void setNerezident(Boolean Nerezident) { this.Nerezident = Nerezident; }
        public Boolean getObosoblennoePodrazdelenie() { return ObosoblennoePodrazdelenie; }
        public void setObosoblennoePodrazdelenie(Boolean ObosoblennoePodrazdelenie) { this.ObosoblennoePodrazdelenie = ObosoblennoePodrazdelenie; }
        public String getYurFizLitso() { return YurFizLitso; }
        public void setYurFizLitso(String YurFizLitso) { this.YurFizLitso = YurFizLitso; }
        public String getOsnovnoyBankovskiySchet() { return OsnovnoyBankovskiySchet; }
        public void setOsnovnoyBankovskiySchet(String OsnovnoyBankovskiySchet) { this.OsnovnoyBankovskiySchet = OsnovnoyBankovskiySchet; }
        public Timestamp getLoadTimestamp() { return LoadTimestamp; }
        public void setLoadTimestamp(Timestamp LoadTimestamp) { this.LoadTimestamp = LoadTimestamp; }
    }
}

