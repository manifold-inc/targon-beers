package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/parquet-go/parquet-go"
	"github.com/planetscale/planetscale-go/planetscale"
	"go.uber.org/zap"
)

var (
	db *sql.DB

	pscaleClient *planetscale.Client
	logger       *zap.SugaredLogger
)

func init() {
	zapLogger, err := zap.NewProduction()
	if err != nil {
		panic("Failed to get logger")
	}
	logger = zapLogger.Sugar()
	defer func() {
		if syncErr := logger.Sync(); syncErr != nil {
			logger.Warnw("Failed to sync logger", "error", syncErr)
		}
	}()

	db, err = sql.Open("mysql", os.Getenv("DSN"))
	if err != nil {
		logger.Fatalw("Failed to connect to database",
			"error", err,
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		logger.Fatalw("Failed to ping database",
			"error", err,
		)
	}
	logger.Info("Successfully connected to PlanetScale database")

	pscaleClient, err = planetscale.NewClient(
		planetscale.WithServiceToken(
			os.Getenv("PLANETSCALE_TOKEN_ID"),
			os.Getenv("PLANETSCALE_TOKEN"),
		),
	)
	if err != nil {
		logger.Fatalw("Failed to create PlanetScale client",
			"error", err,
		)
	}
}

func calculateAndInsertDailyStats(ctx context.Context, logger *zap.SugaredLogger) error {
	logger.Info("Starting Daily Stats")

	query := `
		SELECT 
			model_name,
			SUM(response_tokens) AS total_tokens,
			DATE(created_at) AS created_at,
			AVG(response_tokens / (total_time / 1000)) as avg_tps
		FROM 
			request
		WHERE 
			created_at >= CURDATE() - INTERVAL 1 Day
			AND created_at < CURDATE()
			AND total_time > 0
			AND response_tokens > 0
		GROUP BY 
			model_name,
			DATE(created_at)
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query daily stats: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		logger.Info("No data found for yesterday")
		return nil
	}

	insertQuery := `
		INSERT INTO daily_model_token_counts
		(created_at, model_name, total_tokens, avg_tps)
		VALUES (?, ?, ?, ?)
	`

	for rows.Next() {
		var modelName string
		var totalTokens int64
		var createdAt string
		var avgTps float64

		if err := rows.Scan(&modelName, &totalTokens, &createdAt, &avgTps); err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		_, err := db.ExecContext(ctx, insertQuery, createdAt, modelName, totalTokens, avgTps)
		if err != nil {
			return fmt.Errorf("failed to insert daily stats: %v", err)
		}

		logger.Infow("Inserted daily stats",
			"model", modelName,
			"date", createdAt,
			"tokens", totalTokens,
			"avg_tps", avgTps,
		)
	}

	return nil
}

func disableExpiredModels(ctx context.Context, logger *zap.SugaredLogger) error {
	logger.Info("Starting to check for expired models")

	query := `
		UPDATE model
		SET enabled = FALSE
		WHERE enabled = TRUE
		AND enabled_date <= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
		AND force_enabled = FALSE
		AND NOT EXISTS (
			SELECT 1 
			FROM model_subscription 
			WHERE model_subscription.model_id = model.id 
			AND model_subscription.status = 'active'
		)
	`

	result, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to disable expired models: %v", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get affected rows: %v", err)
	}

	logger.Infow("Disabled expired models",
		"count", affected,
	)
	return nil
}

func waitForBranchReady(ctx context.Context, logger *zap.SugaredLogger, branchName string) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for branch to be ready")
		default:
			branch, err := pscaleClient.DatabaseBranches.Get(ctx, &planetscale.GetDatabaseBranchRequest{
				Organization: os.Getenv("PLANETSCALE_ORG"),
				Database:     os.Getenv("PLANETSCALE_DATABASE"),
				Branch:       branchName,
			})
			if err != nil {
				return fmt.Errorf("failed to check branch status: %v", err)
			}

			if branch.Ready {
				return nil
			}

			logger.Infow("Waiting for branch to be ready",
				"branch", branchName,
				"status", branch.Ready,
			)

			time.Sleep(5 * time.Second)
		}
	}
}

func waitForDeployRequest(ctx context.Context, logger *zap.SugaredLogger, deployReq *planetscale.DeployRequest, branchName string) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for deploy request")
		default:
			status, err := pscaleClient.DeployRequests.Get(ctx, &planetscale.GetDeployRequestRequest{
				Organization: os.Getenv("PLANETSCALE_ORG"),
				Database:     os.Getenv("PLANETSCALE_DATABASE"),
				Number:       deployReq.Number,
			})
			if err != nil {
				return fmt.Errorf("failed to check deploy status: %v", err)
			}

			switch status.State {
			case "complete":
				return nil
			case "canceled", "error":
				logger.Errorw("Deploy request failed",
					"state", status.State,
					"branch", branchName,
					"deploy_number", deployReq.Number,
				)
				return fmt.Errorf("deploy request failed with state: %s", status.State)
			case "pending":
				logger.Infow("Waiting for deploy request",
					"branch", branchName,
					"deploy_number", deployReq.Number,
					"state", status.State,
				)
				time.Sleep(5 * time.Second)
			default:
				logger.Warnw("Unknown deploy request state",
					"state", status.State,
					"branch", branchName,
					"deploy_number", deployReq.Number,
				)
				time.Sleep(5 * time.Second)
			}
		}
	}
}

func deleteOldRequests(ctx context.Context, logger *zap.SugaredLogger, daysOld int, batchSize int) error {
	currentDate := time.Now().Format("2006-01-02")
	// Calculate the date range for the data being deleted
	cutoffDate := time.Now().AddDate(0, 0, -daysOld).Format("2006-01-02")

	logger.Infow("Starting to delete old request records",
		"date", currentDate,
		"cutoff_date", cutoffDate,
		"days_old", daysOld,
		"batch_size", batchSize,
	)

	totalDeleted := int64(0)

	for {
		result, err := db.ExecContext(ctx, `
			DELETE FROM request 
			WHERE created_at < DATE_SUB(CURDATE(), INTERVAL ? DAY)
			LIMIT ?
		`, daysOld, batchSize)
		if err != nil {
			return fmt.Errorf("failed to delete batch: %v", err)
		}

		rowsDeleted, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get affected rows: %v", err)
		}

		totalDeleted += rowsDeleted
		logger.Infow("Deleted batch of records",
			"date", currentDate,
			"batch_size", rowsDeleted,
			"total_deleted", totalDeleted,
		)

		if rowsDeleted < int64(batchSize) {
			break
		}
	}

	logger.Infow("Deletion complete",
		"date", currentDate,
		"total_records", totalDeleted,
	)

	branchName := fmt.Sprintf("cleanup-requests-%d-days-%s",
		daysOld,
		time.Now().Format("20060102"),
	)

	_, err := pscaleClient.DatabaseBranches.Create(ctx, &planetscale.CreateDatabaseBranchRequest{
		Organization: os.Getenv("PLANETSCALE_ORG"),
		Database:     os.Getenv("PLANETSCALE_DATABASE"),
		Name:         branchName,
	})
	if err != nil {
		return fmt.Errorf("failed to create branch: %v", err)
	}

	branchCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()

	if err := waitForBranchReady(branchCtx, logger, branchName); err != nil {
		return err
	}

	password, err := pscaleClient.Passwords.Create(ctx, &planetscale.DatabaseBranchPasswordRequest{
		Organization: os.Getenv("PLANETSCALE_ORG"),
		Database:     os.Getenv("PLANETSCALE_DATABASE"),
		Branch:       branchName,
		DisplayName:  fmt.Sprintf("cleanup-temp-%s", time.Now().Format("20060102150405")),
	})
	if err != nil {
		return fmt.Errorf("failed to create branch password: %v", err)
	}

	logger.Infow("Created branch password",
		"branch", branchName,
		"password_id", password.PublicID,
		"username", password.Username,
		"role", password.Role,
	)

	dbName := os.Getenv("PLANETSCALE_DATABASE")
	branchDSN := password.ConnectionStrings.Go

	logger.Infow("Connecting to branch database",
		"branch", branchName,
		"connection_string", branchDSN,
		"database", dbName,
		"username", password.Username,
	)

	branchDB, err := sql.Open("mysql", branchDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to branch: %v", err)
	}
	defer branchDB.Close()

	pingCtx, pingCancel := context.WithTimeout(ctx, 10*time.Second)
	defer pingCancel()
	if err = branchDB.PingContext(pingCtx); err != nil {
		return fmt.Errorf("failed to ping branch database: %v", err)
	}

	alterQuery := fmt.Sprintf("ALTER TABLE request COMMENT 'Optimize table size via DR - %s';",
		time.Now().Format("2006-01-02"))

	alterCtx, alterCancel := context.WithTimeout(ctx, 30*time.Minute)
	defer alterCancel()

	_, err = branchDB.ExecContext(alterCtx, alterQuery)
	if err != nil {
		return fmt.Errorf("failed to alter table comment on branch: %v", err)
	}

	logger.Infow("Executed ALTER TABLE on branch",
		"branch", branchName,
		"query", alterQuery,
	)

	defer func() {
		err := pscaleClient.Passwords.Delete(ctx, &planetscale.DeleteDatabaseBranchPasswordRequest{
			Organization: os.Getenv("PLANETSCALE_ORG"),
			Database:     os.Getenv("PLANETSCALE_DATABASE"),
			Branch:       branchName,
			PasswordId:   password.PublicID,
		})

		if err != nil {
			logger.Warnw("Failed to delete temporary branch password",
				"error", err,
				"branch", branchName,
				"password_id", password.PublicID,
			)
		} else {
			logger.Infow("Cleaned up temporary branch password",
				"branch", branchName,
				"password_id", password.PublicID,
			)
		}
	}()

	deployReq, err := pscaleClient.DeployRequests.Create(ctx, &planetscale.CreateDeployRequestRequest{
		Organization: os.Getenv("PLANETSCALE_ORG"),
		Database:     os.Getenv("PLANETSCALE_DATABASE"),
		Branch:       branchName,
		IntoBranch:   "main",
		Notes:        fmt.Sprintf("Space reclamation after deleting %d request records older than %d days", totalDeleted, daysOld),
	})
	if err != nil {
		return fmt.Errorf("failed to create deploy request: %v", err)
	}

	deployCtx, deployCancel := context.WithTimeout(ctx, 30*time.Minute)
	defer deployCancel()

	if err := waitForDeployRequest(deployCtx, logger, deployReq, branchName); err != nil {
		return err
	}

	// Only proceed with branch deletion if deploy was successful
	err = pscaleClient.DatabaseBranches.Delete(ctx, &planetscale.DeleteDatabaseBranchRequest{
		Organization: os.Getenv("PLANETSCALE_ORG"),
		Database:     os.Getenv("PLANETSCALE_DATABASE"),
		Branch:       branchName,
	})
	if err != nil {
		logger.Warnw("Failed to delete branch after successful deploy",
			"error", err,
			"branch", branchName,
		)
	} else {
		logger.Infow("Successfully deleted branch after deploy",
			"branch", branchName,
		)
	}

	return nil
}

// archiveOldRequests archives request records older than a specified number of days into a parquet file
func archiveOldRequests(ctx context.Context, logger *zap.SugaredLogger, daysOld int, batchSize int) (string, int, error) {
	currentDate := time.Now().Format("2006-01-02")
	// Calculate the date range for the data being archived
	cutoffDate := time.Now().AddDate(0, 0, -daysOld).Format("2006-01-02")

	logger.Infow("Starting to archive old request records",
		"date", currentDate,
		"cutoff_date", cutoffDate,
		"days_old", daysOld,
		"batch_size", batchSize,
	)

	archivePath := "/data/archives"
	if err := os.MkdirAll(archivePath, 0755); err != nil {
		return "", 0, fmt.Errorf("failed to create archive directory: %v", err)
	}

	// Create parquet file with precise date range in the filename
	parquetFilePath := fmt.Sprintf("%s/requests_older_than_%s.parquet",
		archivePath,
		cutoffDate,
	)

	// Open file
	file, err := os.Create(parquetFilePath)
	if err != nil {
		return "", 0, fmt.Errorf("failed to create file: %v", err)
	}
	defer file.Close()

	type RequestArchive struct {
		PubID     string `parquet:"pub_id"`
		Request   []byte `parquet:"request"`
		Response  []byte `parquet:"response"`
		ModelName string `parquet:"model_name"`
	}

	// Create parquet writer using GenericWriter with RequestArchive type
	writer := parquet.NewGenericWriter[RequestArchive](file)
	// Make sure writer is properly closed when function returns
	defer func() {
		logger.Info("Closing parquet writer")
		if err := writer.Close(); err != nil {
			logger.Errorw("Failed to close parquet writer", "error", err)
		} else {
			logger.Info("Successfully closed parquet writer")
		}
	}()

	totalArchived := 0
	query := `
		SELECT pub_id, request, response, model_name
		FROM request 
		WHERE created_at < DATE_SUB(CURDATE(), INTERVAL ? DAY)
		AND response IS NOT NULL
		LIMIT ?
	`

	// Process data in batches
	for {
		queryCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)

		// Query the database for a batch of records
		rows, err := db.QueryContext(queryCtx, query, daysOld, batchSize)
		if err != nil {
			cancel()
			return parquetFilePath, totalArchived, fmt.Errorf("failed to query records: %v", err)
		}

		var records []RequestArchive
		recordCount := 0

		// process rows
		for rows.Next() {
			var record RequestArchive
			if err := rows.Scan(&record.PubID, &record.Request, &record.Response, &record.ModelName); err != nil {
				rows.Close()
				cancel()
				return parquetFilePath, totalArchived, fmt.Errorf("failed to scan record: %v", err)
			}
			records = append(records, record)
			recordCount++
		}

		// Check for errors during rows iteration
		if err = rows.Err(); err != nil {
			rows.Close()
			cancel()
			return parquetFilePath, totalArchived, fmt.Errorf("error during rows iteration: %v", err)
		}

		rows.Close()
		cancel()

		// Write the batch of records at once using GenericWriter
		if _, err := writer.Write(records); err != nil {
			return parquetFilePath, totalArchived, fmt.Errorf("failed to write batch to parquet: %v", err)
		}

		totalArchived += recordCount
		logger.Infow("Archived batch of records", "batch_size", recordCount, "total", totalArchived)

		// Flush the writer periodically after each batch to ensure data is written
		if flushErr := writer.Flush(); flushErr != nil {
			logger.Warnw("Failed to flush writer", "error", flushErr)
		} else {
			logger.Infow("Successfully flushed batch to disk", "batch_size", recordCount)
		}

		// If fewer records than the batch size were found, we've reached the end
		if recordCount < batchSize {
			logger.Info("Reached the end of records to archive")
			break
		}
	}

	return parquetFilePath, totalArchived, nil
}

// verifyArchiveIntegrity checks that the Parquet file is valid and contains the expected number of records
func verifyArchiveIntegrity(logger *zap.SugaredLogger, filePath string, expectedCount int) (bool, error) {
	logger.Infow("Verifying archive integrity",
		"file_path", filePath,
		"expected_count", expectedCount,
	)

	if expectedCount == 0 {
		logger.Info("No records were archived, skipping verification")
		return true, nil
	}

	file, err := os.Open(filePath)
	if err != nil {
		return false, fmt.Errorf("failed to open archive file for verification: %v", err)
	}
	defer file.Close()

	// Read the Parquet file
	reader := parquet.NewReader(file)
	if reader == nil {
		return false, fmt.Errorf("failed to create Parquet reader, reader is nil")
	}

	// Get the number of rows
	actualCount := reader.NumRows()

	logger.Infow("Archive verification results",
		"file_path", filePath,
		"expected_count", expectedCount,
		"actual_count", actualCount,
	)

	// Verify the row count matches what we expect
	if int64(expectedCount) != actualCount {
		return false, fmt.Errorf("archive integrity check failed: expected %d records, found %d",
			expectedCount, actualCount)
	}

	return true, nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()

	if err := calculateAndInsertDailyStats(ctx, logger); err != nil {
		logger.Errorw("Error calculating daily stats",
			"error", err,
		)
	}

	if err := disableExpiredModels(ctx, logger); err != nil {
		logger.Errorw("Error disabling expired models",
			"error", err,
		)
	}

	// Configuration for data retention
	daysToRetain := 2
	archiveBatchSize := 100
	deleteBatchSize := 100

	// archive old requests
	archivePath, totalArchived, err := archiveOldRequests(ctx, logger, daysToRetain, archiveBatchSize)
	if err != nil {
		logger.Errorw("Error archiving old requests - skipping deletion",
			"error", err,
			"archived_count", totalArchived,
			"archive_path", archivePath,
		)
		// Skip to database close since archiving failed
		if err := db.Close(); err != nil {
			logger.Errorw("Error closing database connection", "error", err)
		}
		return
	}

	logger.Infow("Successfully archived old requests",
		"archived_count", totalArchived,
		"archive_path", archivePath,
	)

	// verify archive before deletings
	archiveValid, verifyErr := verifyArchiveIntegrity(logger, archivePath, totalArchived)
	if verifyErr != nil {
		logger.Errorw("Error verifying archive integrity - skipping deletion",
			"error", verifyErr,
			"archive_path", archivePath,
		)
		if err := db.Close(); err != nil {
			logger.Errorw("Error closing database connection", "error", err)
		}
		return
	}

	if !archiveValid {
		logger.Warn("Archive integrity check failed - skipping deletion")
		if err := db.Close(); err != nil {
			logger.Errorw("Error closing database connection", "error", err)
		}
		return
	}

	if err := deleteOldRequests(ctx, logger, daysToRetain, deleteBatchSize); err != nil {
		logger.Errorw("Error deleting old requests",
			"error", err,
		)
	}

	// Close database connection
	if err := db.Close(); err != nil {
		logger.Errorw("Error closing database connection", "error", err)
	}
}
