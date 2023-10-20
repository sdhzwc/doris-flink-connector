// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package org.apache.doris.flink.tools.cdc;

import org.apache.doris.flink.catalog.doris.DorisSystem;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.catalog.doris.TableSchema;
import org.apache.doris.flink.cfg.DorisConnectionOptions;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.JsonDebeziumSchemaSerializer;
import org.apache.doris.flink.table.DorisConfigOptions;
import org.apache.doris.flink.tools.cdc.mysql.ParsingProcessFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public abstract class DatabaseSync {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseSync.class);
    private static final String LIGHT_SCHEMA_CHANGE = "light_schema_change";
    private static final String TABLE_NAME_OPTIONS = "table-name";
    public static final String TABLE_NAME_TO_LOWER_CASE = "1";
    public static final String TABLE_NAME_TO_UPPER_CASE = "2";
    public static final String TABLE_FIELD_TO_LOWER_CASE = "1";
    public static final String TABLE_FIELD_TO_UPPER_CASE = "2";
    protected Configuration config;
    protected String database;
    protected TableNameConverter converter;
    protected Pattern includingPattern;
    protected Pattern excludingPattern;
    protected Map<String, String> tableConfig;
    protected Configuration sinkConfig;
    protected boolean ignoreDefaultValue;
    public StreamExecutionEnvironment env;
    private boolean createTableOnly = false;
    private boolean newSchemaChange;
    protected String includingTables;
    protected String excludingTables;
    protected String tableNameCaseConversion;
    protected String tableFieldCaseConversion;

    public abstract Connection getConnection() throws SQLException;

    public abstract List<SourceSchema> getSchemaList() throws Exception;

    public abstract DataStreamSource<String> buildCdcSource(StreamExecutionEnvironment env);

    public void create(StreamExecutionEnvironment env, String database, Configuration config,
                       String tablePrefix, String tableSuffix, String tableNameCaseConversion,
                       String tableFieldCaseConversion, String includingTables,
                       String excludingTables, boolean ignoreDefaultValue, Configuration sinkConfig,
            Map<String, String> tableConfig, boolean createTableOnly, boolean useNewSchemaChange) {
        this.env = env;
        this.config = config;
        this.database = database;
        this.converter = new TableNameConverter(tablePrefix, tableSuffix, tableNameCaseConversion, tableFieldCaseConversion);
        this.tableNameCaseConversion = tableNameCaseConversion == null ? "" : tableNameCaseConversion;
        this.tableFieldCaseConversion = tableFieldCaseConversion == null ? "" : tableFieldCaseConversion;
        this.includingTables = includingTables;
        this.excludingTables = excludingTables;
        this.includingPattern = includingTables == null ? null : Pattern.compile(includingTables);
        this.excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
        this.ignoreDefaultValue = ignoreDefaultValue;
        this.sinkConfig = sinkConfig;
        this.tableConfig = tableConfig == null ? new HashMap<>() : tableConfig;
        //default enable light schema change
        if(!this.tableConfig.containsKey(LIGHT_SCHEMA_CHANGE)){
            this.tableConfig.put(LIGHT_SCHEMA_CHANGE, "true");
        }
        this.createTableOnly = createTableOnly;
        this.newSchemaChange = useNewSchemaChange;
    }

    public void build() throws Exception {
        DorisConnectionOptions options = getDorisConnectionOptions();
        DorisSystem dorisSystem = new DorisSystem(options);

        List<SourceSchema> schemaList = getSchemaList();
        Preconditions.checkState(!schemaList.isEmpty(), "No tables to be synchronized.");
        if (!dorisSystem.databaseExists(database)) {
            LOG.info("database {} not exist, created", database);
            dorisSystem.createDatabase(database);
        }

        List<String> syncTables = new ArrayList<>();
        List<String> dorisTables = new ArrayList<>();
        for (SourceSchema schema : schemaList) {
            syncTables.add(schema.getTableName());
            String dorisTable = converter.convert(schema.getTableName());
            if (!dorisSystem.tableExists(database, dorisTable)) {
                TableSchema dorisSchema = schema.convertTableSchema(tableConfig);
                dorisSchema.setKeys(keysConvert(dorisSchema.getKeys()));
                dorisSchema.setDistributeKeys(keysConvert(dorisSchema.getDistributeKeys()));
                dorisSchema.setFields(tableFieldsConvert(dorisSchema.getFields()));
                //set doris target database
                dorisSchema.setDatabase(database);
                dorisSchema.setTable(dorisTable);
                dorisSystem.createTable(dorisSchema);
            }
            dorisTables.add(dorisTable);
        }
        if(createTableOnly){
            System.out.println("Create table finished.");
            System.exit(0);
        }

        config.setString(TABLE_NAME_OPTIONS, "(" + String.join("|", syncTables) + ")");
        DataStreamSource<String> streamSource = buildCdcSource(env);
        SingleOutputStreamOperator<Void> parsedStream = streamSource.process(new ParsingProcessFunction(converter));
        for (String table : dorisTables) {
            OutputTag<String> recordOutputTag = ParsingProcessFunction.createRecordOutputTag(table);
            DataStream<String> sideOutput = parsedStream.getSideOutput(recordOutputTag);

            int sinkParallel = sinkConfig.getInteger(DorisConfigOptions.SINK_PARALLELISM, sideOutput.getParallelism());
            sideOutput.sinkTo(buildDorisSink(table)).setParallelism(sinkParallel).name(table).uid(table);
        }
    }

    private DorisConnectionOptions getDorisConnectionOptions() {
        String fenodes = sinkConfig.getString(DorisConfigOptions.FENODES);
        String benodes = sinkConfig.getString(DorisConfigOptions.BENODES);
        String user = sinkConfig.getString(DorisConfigOptions.USERNAME);
        String passwd = sinkConfig.getString(DorisConfigOptions.PASSWORD, "");
        String jdbcUrl = sinkConfig.getString(DorisConfigOptions.JDBC_URL);
        Preconditions.checkNotNull(fenodes, "fenodes is empty in sink-conf");
        Preconditions.checkNotNull(user, "username is empty in sink-conf");
        Preconditions.checkNotNull(jdbcUrl, "jdbcurl is empty in sink-conf");
        DorisConnectionOptions.DorisConnectionOptionsBuilder builder = new DorisConnectionOptions.DorisConnectionOptionsBuilder()
                .withFenodes(fenodes)
                .withBenodes(benodes)
                .withUsername(user)
                .withPassword(passwd)
                .withJdbcUrl(jdbcUrl);
        return builder.build();
    }

    /**
     * create doris sink
     */
    public DorisSink<String> buildDorisSink(String table) {
        String fenodes = sinkConfig.getString(DorisConfigOptions.FENODES);
        String benodes = sinkConfig.getString(DorisConfigOptions.BENODES);
        String user = sinkConfig.getString(DorisConfigOptions.USERNAME);
        String passwd = sinkConfig.getString(DorisConfigOptions.PASSWORD, "");
        String labelPrefix = sinkConfig.getString(DorisConfigOptions.SINK_LABEL_PREFIX);

        DorisSink.Builder<String> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes(fenodes)
                .setBenodes(benodes)
                .setTableIdentifier(database + "." + table)
                .setUsername(user)
                .setPassword(passwd);
        sinkConfig.getOptional(DorisConfigOptions.AUTO_REDIRECT).ifPresent(dorisBuilder::setAutoRedirect);

        Properties pro = new Properties();
        //default json data format
        pro.setProperty("format", "json");
        pro.setProperty("read_json_by_line", "true");
        //customer stream load properties
        Properties streamLoadProp = DorisConfigOptions.getStreamLoadProp(sinkConfig.toMap());
        pro.putAll(streamLoadProp);
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder()
                .setLabelPrefix(String.join("-", labelPrefix, database, table))
                .setStreamLoadProp(pro);

        sinkConfig.getOptional(DorisConfigOptions.SINK_ENABLE_DELETE).ifPresent(executionBuilder::setDeletable);
        sinkConfig.getOptional(DorisConfigOptions.SINK_BUFFER_COUNT).ifPresent(executionBuilder::setBufferCount);
        sinkConfig.getOptional(DorisConfigOptions.SINK_BUFFER_SIZE).ifPresent(executionBuilder::setBufferSize);
        sinkConfig.getOptional(DorisConfigOptions.SINK_CHECK_INTERVAL).ifPresent(executionBuilder::setCheckInterval);
        sinkConfig.getOptional(DorisConfigOptions.SINK_MAX_RETRIES).ifPresent(executionBuilder::setMaxRetries);
        sinkConfig.getOptional(DorisConfigOptions.SINK_IGNORE_UPDATE_BEFORE).ifPresent(executionBuilder::setIgnoreUpdateBefore);


        if(!sinkConfig.getBoolean(DorisConfigOptions.SINK_ENABLE_2PC)){
            executionBuilder.disable2PC();
        } else if(sinkConfig.getOptional(DorisConfigOptions.SINK_ENABLE_2PC).isPresent()){
            //force open 2pc
            executionBuilder.enable2PC();
        }

        //batch option
        if(sinkConfig.getBoolean(DorisConfigOptions.SINK_ENABLE_BATCH_MODE)){
            executionBuilder.enableBatchMode();
        }
        sinkConfig.getOptional(DorisConfigOptions.SINK_FLUSH_QUEUE_SIZE).ifPresent(executionBuilder::setFlushQueueSize);
        sinkConfig.getOptional(DorisConfigOptions.SINK_BUFFER_FLUSH_MAX_ROWS).ifPresent(executionBuilder::setBufferFlushMaxRows);
        sinkConfig.getOptional(DorisConfigOptions.SINK_BUFFER_FLUSH_MAX_BYTES).ifPresent(executionBuilder::setBufferFlushMaxBytes);
        sinkConfig.getOptional(DorisConfigOptions.SINK_BUFFER_FLUSH_INTERVAL).ifPresent(v-> executionBuilder.setBufferFlushIntervalMs(v.toMillis()));

        sinkConfig.getOptional(DorisConfigOptions.SINK_USE_CACHE).ifPresent(executionBuilder::setUseCache);

        DorisExecutionOptions executionOptions = executionBuilder.build();
        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(JsonDebeziumSchemaSerializer.builder()
                        .setDorisOptions(dorisBuilder.build())
                        .setNewSchemaChange(newSchemaChange)
                        .setExecutionOptions(executionOptions)
                        .setTableFieldCaseConversion(tableFieldCaseConversion)
                        .build())
                .setDorisOptions(dorisBuilder.build());
        return builder.build();
    }

    /**
     * Filter table that need to be synchronized
     */
    protected boolean isSyncNeeded(String tableName) {
        boolean sync = true;
        if (includingPattern != null) {
            sync = includingPattern.matcher(tableName).matches();
        }
        if (excludingPattern != null) {
            sync = sync && !excludingPattern.matcher(tableName).matches();
        }
        LOG.debug("table {} is synchronized? {}", tableName, sync);
        return sync;
    }

    public static class TableNameConverter implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String prefix;
        private final String suffix;
        private final String tableNameCaseConversion;
        private final String tableFieldCaseConversion;

        TableNameConverter(){
            this("","","","");
        }

        TableNameConverter(String prefix, String suffix, String tableNameCaseConversion, String tableFieldCaseConversion) {
            this.prefix = prefix == null ? "" : prefix;
            this.suffix = suffix == null ? "" : suffix;
            this.tableNameCaseConversion = tableNameCaseConversion;
            this.tableFieldCaseConversion = tableFieldCaseConversion;
        }

        public String convert(String tableName) {
            return prefix + tableNameConvert(tableName) + suffix;
        }

        public String getTableFieldCaseConversion() {
            return tableFieldCaseConversion;
        }

        public  String tableNameConvert(String tableName) {
            if (TABLE_NAME_TO_LOWER_CASE.equals(tableNameCaseConversion)){
                tableName = tableName.toLowerCase();
            }
            if (TABLE_NAME_TO_UPPER_CASE.equals(tableNameCaseConversion)){
                tableName = tableName.toUpperCase();
            }
            return tableName;
        }

        public String tableFieldConvert(String tableField) {
            if (TABLE_FIELD_TO_LOWER_CASE.equals(tableFieldCaseConversion)){
                tableField = tableField.toLowerCase();
            }
            if (TABLE_FIELD_TO_UPPER_CASE.equals(tableFieldCaseConversion)){
                tableField = tableField.toUpperCase();
            }
            return tableField;
        }
    }

    private List<String> keysConvert(List<String> keys) {
        return keys.stream().map(this::tableFieldConvert).collect(Collectors.toList());
    }

    private Map<String, FieldSchema> tableFieldsConvert(Map<String, FieldSchema> fields) {
        Map<String, FieldSchema> newFiles = new LinkedHashMap<>();
        for (Map.Entry<String, FieldSchema> fieldSchemaEntry : fields.entrySet()) {
            String fileKey = fieldSchemaEntry.getKey();
            if (fileKey != null) {
                fileKey = tableFieldConvert(fileKey);
            }
            FieldSchema fileValue = getFieldSchemaConvert(fieldSchemaEntry);
            newFiles.put(fileKey, fileValue);
        }
        return newFiles;
    }

    private FieldSchema getFieldSchemaConvert(Map.Entry<String, FieldSchema> fieldSchemaEntry) {
        FieldSchema fileValue = fieldSchemaEntry.getValue();
        String name = fileValue.getName();
        if (name != null) {
            fileValue.setName(tableFieldConvert(name));
        }
        String comment = fileValue.getComment();
        fileValue.setComment(tableFieldConvert(comment));
        return fileValue;
    }

    public String tableFieldConvert(String tableField) {
        if (TABLE_FIELD_TO_LOWER_CASE.equals(tableFieldCaseConversion)){
            tableField = tableField.toLowerCase();
        }
        if (TABLE_FIELD_TO_UPPER_CASE.equals(tableFieldCaseConversion)){
            tableField = tableField.toUpperCase();
        }
        return tableField;
    }
}
