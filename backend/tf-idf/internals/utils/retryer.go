package utils

import (
	"context"
	"errors"
	"net"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func IsRetryableNetworkError(err error) bool {
	var netErr net.Error

	return errors.As(err, &netErr)
}

func IsRetryablePostgresError(err error) bool {
	if errors.Is(err, pgx.ErrNoRows) {
		return false
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// Class 08 — Connection Exception
		// Class 57 — Operator Intervention (shutdown, etc.)
		if strings.HasPrefix(pgErr.Code, "08") || strings.HasPrefix(pgErr.Code, "57") {
			return true
		}

		// Serialization failures and deadlocks are retryable
		// Class 40 — Transaction Rollback
		if strings.HasPrefix(pgErr.Code, "40") {
			return true
		}

		return false
	}

	// Connection-specific errors
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return true
	}

	return IsRetryableNetworkError(err)
}
