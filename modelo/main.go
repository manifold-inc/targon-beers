package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/planetscale/planetscale-go/planetscale"
	"go.uber.org/zap"
)

var (
	db           *sql.DB
	pscaleClient *planetscale.Client
	logger       *zap.SugaredLogger
)

func init() {
	zapLogger, err := zap.NewProduction()
	if err != nil {
		panic("Failed to get logger")
	}
	logger = zapLogger.Sugar()
	defer zapLogger.Sync()

	db, err = sql.Open("mysql", os.Getenv("DSN"))
	if err != nil {
		logger.Fatalw("Failed to connect to database",
			"error", err,
		)
	}

	// timeout
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
		var createdAt time.Time
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
			"date", createdAt.Format("2006-01-02"),
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

func deleteOldRequests(ctx context.Context, logger *zap.SugaredLogger) error {
	currentDate := time.Now().Format("2006-01-02")
	logger.Infow("Starting to delete old request records",
		"date", currentDate,
	)

	batchSize := 500
	totalDeleted := int64(0)

	for {
		result, err := db.ExecContext(ctx, `
			DELETE FROM request 
			WHERE created_at < DATE_SUB(CURDATE(), INTERVAL 2 DAY)
			LIMIT ?
		`, batchSize)
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

	branchName := fmt.Sprintf("cleanup-requests-%s-days-%s",
		"2",
		time.Now().Format("20060102"),
	)

	// create branch
	branch, err := pscaleClient.DatabaseBranches.Create(ctx, &planetscale.CreateDatabaseBranchRequest{
		Organization: os.Getenv("PLANETSCALE_ORG"),
		Database:     os.Getenv("PLANETSCALE_DATABASE"),
		Name:         branchName,
	})
	if err != nil {
		return fmt.Errorf("failed to create branch: %v", err)
	}

	// wait for branch to be ready
	branchCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	for !branch.Ready {
		select {
		case <-branchCtx.Done():
			return fmt.Errorf("timeout waiting for branch to be ready")
		default:
			logger.Infow("Waiting for branch to be ready",
				"branch", branchName,
				"status", branch.Ready,
			)
			time.Sleep(5 * time.Second)

			branch, err = pscaleClient.DatabaseBranches.Get(ctx, &planetscale.GetDatabaseBranchRequest{
				Organization: os.Getenv("PLANETSCALE_ORG"),
				Database:     os.Getenv("PLANETSCALE_DATABASE"),
				Branch:       branchName,
			})
			if err != nil {
				return fmt.Errorf("failed to check branch status: %v", err)
			}
		}
	}

	// after the branch is ready but before creating the deploy request
	alterQuery := fmt.Sprintf("ALTER TABLE request COMMENT 'Optimize table size via DR - %s';",
		time.Now().Format("2006-01-02"))
	_, err = db.ExecContext(ctx, alterQuery)
	if err != nil {
		return fmt.Errorf("failed to alter table comment: %v", err)
	}

	// create deploy request
	deployReq, err := pscaleClient.DeployRequests.Create(ctx, &planetscale.CreateDeployRequestRequest{
		Organization: os.Getenv("PLANETSCALE_ORG"),
		Database:     os.Getenv("PLANETSCALE_DATABASE"),
		Branch:       branchName,
		IntoBranch:   "main",
		Notes:        fmt.Sprintf("Space reclamation after deleting %d request records older than 2 days", totalDeleted),
	})
	if err != nil {
		return fmt.Errorf("failed to create deploy request: %v", err)
	}

	// wait for deploy request to be ready
	deployCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	for deployReq.State == "pending" {
		select {
		case <-deployCtx.Done():
			return fmt.Errorf("timeout waiting for deploy request to be ready")
		default:
			logger.Infow("Waiting for deploy request to be ready",
				"request_number", deployReq.Number,
				"state", deployReq.State,
			)
			time.Sleep(5 * time.Second)

			deployReq, err = pscaleClient.DeployRequests.Get(ctx, &planetscale.GetDeployRequestRequest{
				Organization: os.Getenv("PLANETSCALE_ORG"),
				Database:     os.Getenv("PLANETSCALE_DATABASE"),
				Number:       deployReq.Number,
			})
			if err != nil {
				return fmt.Errorf("failed to check deploy request status: %v", err)
			}
		}
	}

	// auto-apply the deploy request
	_, err = pscaleClient.DeployRequests.AutoApplyDeploy(ctx, &planetscale.AutoApplyDeployRequestRequest{
		Organization: os.Getenv("PLANETSCALE_ORG"),
		Database:     os.Getenv("PLANETSCALE_DATABASE"),
		Number:       deployReq.Number,
		Enable:       true,
	})
	if err != nil {
		return fmt.Errorf("failed to auto-apply deploy request: %v", err)
	}

	logger.Infow("Created and auto-applied deploy request",
		"request_number", deployReq.Number,
		"branch", branchName,
	)
	return nil
}

func verifyRecentBackupExists(ctx context.Context, logger *zap.SugaredLogger) (bool, error) {
	logger.Info("Verifying recent backup exists before proceeding with deletions")

	// list all backups
	backups, err := pscaleClient.Backups.List(ctx, &planetscale.ListBackupsRequest{
		Organization: os.Getenv("PLANETSCALE_ORG"),
		Database:     os.Getenv("PLANETSCALE_DATABASE"),
		Branch:       "main",
	})
	if err != nil {
		return false, fmt.Errorf("failed to list backups: %v", err)
	}

	// check if we have any backups
	if len(backups) == 0 {
		logger.Warn("No backups found")
		return false, nil
	}

	// get current date for comparison in UTC cause timezones are a pain
	now := time.Now().UTC()
	today := now.Format("2006-01-02")
	yesterday := now.AddDate(0, 0, -1).Format("2006-01-02")

	logger.Infow("Checking for backups with dates",
		"today_utc", today,
		"yesterday_utc", yesterday,
		"current_time_utc", now.Format("2006-01-02 15:04:05"),
	)

	var inProgressBackup *planetscale.Backup
	var mostRecentBackup *planetscale.Backup

	for _, backup := range backups {
		backupDate := backup.CreatedAt.UTC().Format("2006-01-02")

		logger.Infow("Found backup",
			"backup_id", backup.PublicID,
			"created_at", backup.CreatedAt.UTC().Format("2006-01-02 15:04:05"),
			"backup_date", backupDate,
			"state", backup.State,
			"size", backup.Size,
		)

		// check for backups before deleting
		if backupDate == today {
			if backup.State == "success" {
				logger.Infow("Today's backup is complete",
					"backup_id", backup.PublicID,
					"created_at", backup.CreatedAt.UTC().Format("2006-01-02 15:04:05"),
					"size", backup.Size,
					"state", backup.State,
				)
				return true, nil
			} else if backup.State == "running" || backup.State == "pending" {
				inProgressBackup = backup
			}
		}

		if mostRecentBackup == nil || backup.CreatedAt.After(mostRecentBackup.CreatedAt) {
			if backup.State == "success" {
				mostRecentBackup = backup
			}
		}
	}

	// if today's backup is in progress, use yesterday's backup as fallback
	if inProgressBackup != nil {
		logger.Infow("Today's backup is still in progress",
			"backup_id", inProgressBackup.PublicID,
			"started_at", inProgressBackup.StartedAt.UTC().Format("2006-01-02 15:04:05"),
			"state", inProgressBackup.State,
		)

		for _, backup := range backups {
			backupDate := backup.CreatedAt.UTC().Format("2006-01-02")
			if backupDate == yesterday && backup.State == "success" {
				logger.Infow("Using yesterday's backup as fallback",
					"backup_id", backup.PublicID,
					"created_at", backup.CreatedAt.UTC().Format("2006-01-02 15:04:05"),
					"size", backup.Size,
				)
				return true, nil
			}
		}
	}

	if mostRecentBackup != nil {
		hoursAgo := now.Sub(mostRecentBackup.CreatedAt).Hours()
		if hoursAgo < 48 {
			logger.Infow("Using recent backup",
				"backup_id", mostRecentBackup.PublicID,
				"created_at", mostRecentBackup.CreatedAt.UTC().Format("2006-01-02 15:04:05"),
				"size", mostRecentBackup.Size,
				"hours_ago", fmt.Sprintf("%.1f", hoursAgo),
			)
			return true, nil
		}
	}

	logger.Warn("No recent backups found within the last 48 hours")
	return false, nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
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

	hasRecentBackup, err := verifyRecentBackupExists(ctx, logger)
	if err != nil {
		logger.Errorw("Error verifying recent backup",
			"error", err,
		)
	} else if !hasRecentBackup {
		logger.Warn("Skipping deletion because no recent backup was found")
	} else {
		if err := deleteOldRequests(ctx, logger); err != nil {
			logger.Errorw("Error deleting old requests",
				"error", err,
			)
		}
	}

	// close connections
	if err := db.Close(); err != nil {
		logger.Errorw("Error closing database connection", "error", err)
	}

	if err := logger.Sync(); err != nil {
		fmt.Printf("Error syncing logger: %v\n", err)
	}
}
