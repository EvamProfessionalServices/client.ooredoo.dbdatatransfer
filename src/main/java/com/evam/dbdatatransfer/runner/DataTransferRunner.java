package com.evam.dbdatatransfer.runner;

import com.evam.dbdatatransfer.config.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
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

            // Step 1: Drop partition before insert if required
            if (appProperties.getTransfer().isDropPartitionBeforeInsert()) {
                dropPartitionBeforeInsert();
            }

            // Step 2: Truncate the target table if required
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

    /*
    private void dropPartitionBeforeInsert() {
        String tableName = appProperties.getTransfer().getTargetTable();
        String partitionDate = getPartitionDate(); // Assuming partition by DATE column
        String sql = "ALTER TABLE " + tableName + " DROP PARTITION FOR (TO_DATE('" + partitionDate + "', 'YYYY-MM-DD'))";
        log.info("Dropping partition for date '{}'...", partitionDate);
        targetJdbcTemplate.execute(sql);
        log.info("Partition for date '{}' has been successfully dropped.", partitionDate);
    }

     */


    private void dropPartitionBeforeInsert() {
        String tableName = appProperties.getTransfer().getTargetTable();
        String partitionDate = getPartitionDate(); // Assuming partition by DATE column
        String sql = "ALTER TABLE " + tableName + " DROP PARTITION FOR (TO_DATE('" + partitionDate + "', 'YYYY-MM-DD'))";

        try {
            // Try to drop the partition
            log.info("Dropping partition for date '{}'...", partitionDate);
            targetJdbcTemplate.execute(sql);
            log.info("Partition for date '{}' has been successfully dropped.", partitionDate);
        } catch (Exception e) {
            // Handle the "partition does not exist" error (ORA-02149)
            if (isPartitionSqlError(e)) {
                log.info("Partition for date '{}' does not exist on table '{}'; skipping drop operation.", partitionDate, tableName);
            } else {
                // For other errors, log them but **do not stop** the application
                log.error("Error while dropping partition for date '{}' on table '{}'.", partitionDate, tableName, e);
                // Ensure the exception is handled, but do not exit the process
                // Continue with the rest of the operations
            }
        }
    }

    private boolean isPartitionSqlError(Exception e) {
        // Check if the exception is an instance of SQLException
        if (e instanceof SQLException) {
            // Log the SQLException without checking the error code
            log.warn("Non-critical SQL exception occurred: {}", e.getMessage());
            return true; // This is a non-critical error that should be logged and can continue
        }
        return false; // Not an SQLException, so it is treated as a critical error
    }


    private String getPartitionDate() {
        // Return the current date or logic to determine the specific date for partition
        // Here we are assuming that the partition drop is for the current day
        return java.time.LocalDate.now().toString(); // Formats the date as "YYYY-MM-DD"
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