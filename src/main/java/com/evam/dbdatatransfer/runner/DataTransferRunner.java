package com.evam.dbdatatransfer.runner;

import com.evam.dbdatatransfer.config.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Component
public class DataTransferRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(DataTransferRunner.class);

    private final JdbcTemplate sourceJdbcTemplate;
    private final JdbcTemplate targetJdbcTemplate;
    private final AppProperties appProperties;

    public DataTransferRunner(@Qualifier("sourceJdbcTemplate") JdbcTemplate sourceJdbcTemplate,
                              @Qualifier("targetJdbcTemplate") JdbcTemplate targetJdbcTemplate,
                              AppProperties appProperties) {
        this.sourceJdbcTemplate = sourceJdbcTemplate;
        this.targetJdbcTemplate = targetJdbcTemplate;
        this.appProperties = appProperties;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Data transfer application started.");
        long startTime = System.currentTimeMillis();

        try {
            // Step 1: Truncate the target table if required
            if (appProperties.getTransfer().isTruncateTargetTable()) {
                truncateTargetTable();
            }

            // Step 2: Perform the data transfer
            long totalRowsTransferred = transferData();

            long endTime = System.currentTimeMillis();
            log.info("===============================================================");
            log.info("DATA TRANSFER COMPLETED SUCCESSFULLY!");
            log.info("Total rows transferred: {}", totalRowsTransferred);
            log.info("Total execution time: {} seconds", (endTime - startTime) / 1000.0);
            log.info("===============================================================");

        } catch (Exception e) {
            log.error("A critical error occurred during the data transfer process!", e);
            // Exit with a non-zero status code to indicate failure
            System.exit(1);
        }
    }

    private void truncateTargetTable() {
        String tableName = appProperties.getTransfer().getTargetTable();
        log.info("Truncating target table '{}'...", tableName);
        targetJdbcTemplate.execute("TRUNCATE TABLE " + tableName);
        log.info("Table '{}' has been successfully truncated.", tableName);
    }

    private long transferData() {
        AppProperties.Transfer transferProps = appProperties.getTransfer();
        String selectQuery = transferProps.getSelectQuery();
        int fetchSize = transferProps.getFetchSize();
        int commitSize = transferProps.getCommitSize();

        // Dynamically build the INSERT statement
        String insertQuery = buildInsertQuery();
        log.debug("Generated INSERT query: {}", insertQuery);

        final List<Object[]> batch = new ArrayList<>(commitSize);
        final AtomicLong totalRowCount = new AtomicLong(0);

        log.info("Starting data extraction from source... Fetch Size: {}, Commit Size: {}", fetchSize, commitSize);

        sourceJdbcTemplate.setFetchSize(fetchSize);
        sourceJdbcTemplate.query(selectQuery, rs -> {
            int columnCount = rs.getMetaData().getColumnCount();
            Object[] row = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
                row[i] = rs.getObject(i + 1);
            }
            batch.add(row);

            if (batch.size() >= commitSize) {
                insertBatch(batch, insertQuery);
                totalRowCount.addAndGet(batch.size());
                log.info("{} total rows transferred so far...", totalRowCount.get());
                batch.clear();
            }
        });

        // Process the final remaining batch
        if (!batch.isEmpty()) {
            insertBatch(batch, insertQuery);
            totalRowCount.addAndGet(batch.size());
            log.info("Flushing the final batch of {} rows. Grand total: {}", batch.size(), totalRowCount.get());
            batch.clear();
        }

        return totalRowCount.get();
    }

    private void insertBatch(List<Object[]> batch, String insertQuery) {
        if (batch.isEmpty()) {
            return;
        }
        log.debug("Writing a batch of {} rows to the target...", batch.size());
        targetJdbcTemplate.batchUpdate(insertQuery, batch);
    }

    private String buildInsertQuery() {
        AppProperties.Transfer transferProps = appProperties.getTransfer();
        String tableName = transferProps.getTargetTable();
        String columns = String.join(", ", transferProps.getTargetColumns());
        String placeholders = transferProps.getTargetColumns().stream()
                .map(c -> "?")
                .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, placeholders);
    }
}