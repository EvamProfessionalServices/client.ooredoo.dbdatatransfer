package com.evam.dbdatatransfer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import java.util.List;

@Component
@ConfigurationProperties(prefix = "data-transfer")
public class AppProperties {

    private Database sourceDb;
    private Database targetDb;
    private Transfer transfer;

    // Getters and Setters
    public Database getSourceDb() { return sourceDb; }
    public void setSourceDb(Database sourceDb) { this.sourceDb = sourceDb; }
    public Database getTargetDb() { return targetDb; }
    public void setTargetDb(Database targetDb) { this.targetDb = targetDb; }
    public Transfer getTransfer() { return transfer; }
    public void setTransfer(Transfer transfer) { this.transfer = transfer; }

    public static class Database {
        private String url;
        private String username;
        private String password;
        private String driverClassName;

        // Getters and Setters
        public String getUrl() { return url; }
        public void setUrl(String url) { this.url = url; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public String getDriverClassName() { return driverClassName; }
        public void setDriverClassName(String driverClassName) { this.driverClassName = driverClassName; }
    }

    public static class Transfer {
        private String selectQuery;
        private String targetTable;
        private List<String> targetColumns;
        private int fetchSize;
        private int commitSize;
        private boolean truncateTargetTable;

        // Getters and Setters
        public String getSelectQuery() { return selectQuery; }
        public void setSelectQuery(String selectQuery) { this.selectQuery = selectQuery; }
        public String getTargetTable() { return targetTable; }
        public void setTargetTable(String targetTable) { this.targetTable = targetTable; }
        public List<String> getTargetColumns() { return targetColumns; }
        public void setTargetColumns(List<String> targetColumns) { this.targetColumns = targetColumns; }
        public int getFetchSize() { return fetchSize; }
        public void setFetchSize(int fetchSize) { this.fetchSize = fetchSize; }
        public int getCommitSize() { return commitSize; }
        public void setCommitSize(int commitSize) { this.commitSize = commitSize; }
        public boolean isTruncateTargetTable() { return truncateTargetTable; }
        public void setTruncateTargetTable(boolean truncateTargetTable) { this.truncateTargetTable = truncateTargetTable; }
    }
}