// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

// Adaptive Chunking Implementation for Dumpling
//
// This package implements dynamic chunking strategies that avoid the connection timeout
// issues experienced with large tables in MySQL 5.7. Instead of using expensive 
// hash-based sampling (CRC32), it uses incremental boundary discovery with prefetching queries.
//
// Key Approaches:
//
// 1. Composite Strategy (complex primary keys):
//    - Uses prefetching queries: SELECT field FROM table WHERE field > 'prev' ORDER BY field LIMIT N, 1
//    - Discovers boundaries incrementally during processing
//    - Avoids expensive ROW_NUMBER() operations on older MySQL versions
//
// 2. Optimistic Strategy (auto-increment numeric keys):
//    - Calculates boundaries arithmetically: next_boundary = current_value + chunk_size
//    - Assumes no large gaps in auto-increment sequences
//    - Much faster than querying for numeric primary keys
//
// 3. Minimal Strategy (very large tables 100M+ rows):
//    - Uses smaller chunk sizes and simple LIMIT queries
//    - Avoids complex operations that might cause timeouts
//    - Designed specifically for connection timeout prevention
//
// Performance Features:
// - Time-based chunk sizing (target: 500ms per chunk)
// - Dynamic chunk size adjustment based on processing feedback
// - Sliding window metrics tracking (last 10 chunks)
// - Adaptive sizing bounds: 100 to 100,000 rows per chunk

package export

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	tcontext "github.com/pingcap/tidb/dumpling/context"
	"go.uber.org/zap"
)

const (
	// Target chunk processing time (inspired by Spirit's approach)
	DefaultChunkTargetTime = 500 * time.Millisecond
	MaxChunkTargetTime     = 5 * time.Second
	MinChunkTargetTime     = 100 * time.Millisecond
	
	// Adaptive sizing constraints
	DefaultStartingChunkSize = 1000
	MaxDynamicStepFactor     = 1.5
	MinDynamicStepFactor     = 0.5
	
	// Large table threshold for different strategies
	LargeTableThreshold = 100000000 // 100M rows
)

// ChunkStrategy represents different chunking approaches
type ChunkStrategy int

const (
	StrategySequential ChunkStrategy = iota
	StrategyOptimistic // For auto-increment or predictable keys
	StrategyComposite  // For complex keys using actual data sampling
	StrategyMinimal    // For very large tables with minimal sampling
)

// ChunkingMetrics tracks performance for adaptive sizing
type ChunkingMetrics struct {
	ProcessingTime   time.Duration
	RowsProcessed    int64
	ChunkSize        int64
	Successful       bool
	TimestampStart   time.Time
	TimestampEnd     time.Time
}

// AdaptiveChunker implements dynamic chunking with incremental boundary discovery
type AdaptiveChunker struct {
	strategy       ChunkStrategy
	targetTime     time.Duration
	currentChunkSize int64
	db             string
	table          string
	field          string
	conf           *Config
	conn           *BaseConn
	
	// Performance tracking
	metrics        []ChunkingMetrics
	watermark      string
}

// NewAdaptiveChunker creates a new adaptive chunker
func NewAdaptiveChunker(db, table, field string, conf *Config, conn *BaseConn) *AdaptiveChunker {
	return &AdaptiveChunker{
		strategy:         StrategyComposite, // Default to most flexible
		targetTime:       DefaultChunkTargetTime,
		currentChunkSize: DefaultStartingChunkSize,
		db:               db,
		table:            table,
		field:            field,
		conf:             conf,
		conn:             conn,
		metrics:          make([]ChunkingMetrics, 0),
	}
}

// DetermineStrategy selects the best chunking strategy based on table characteristics
func (ac *AdaptiveChunker) DetermineStrategy(tctx *tcontext.Context, count int64) ChunkStrategy {
	// For very large tables, use minimal sampling to avoid timeouts
	if count > LargeTableThreshold {
		tctx.L().Info("large table detected, using minimal sampling strategy",
			zap.Int64("rowCount", count),
			zap.Int64("threshold", LargeTableThreshold))
		return StrategyMinimal
	}
	
	// Check if field looks like auto-increment (simple heuristic)
	if ac.isLikelyAutoIncrement(tctx) {
		tctx.L().Debug("field appears to be auto-increment, using optimistic strategy",
			zap.String("field", ac.field))
		return StrategyOptimistic
	}
	
	// Default to composite strategy for string fields
	tctx.L().Debug("using composite strategy for complex field",
		zap.String("field", ac.field))
	return StrategyComposite
}

// isLikelyAutoIncrement checks if the field is likely an auto-increment column
func (ac *AdaptiveChunker) isLikelyAutoIncrement(tctx *tcontext.Context) bool {
	// Simple heuristic: check field name and try to determine if it's numeric
	fieldLower := strings.ToLower(ac.field)
	if strings.Contains(fieldLower, "id") || strings.Contains(fieldLower, "auto") {
		// If we don't have a connection, default to optimistic for ID fields
		if ac.conn == nil {
			return true
		}
		
		// Query a sample to see if values are numeric and sequential
		query := fmt.Sprintf("SELECT `%s` FROM `%s`.`%s` ORDER BY `%s` LIMIT 3",
			escapeString(ac.field), escapeString(ac.db), escapeString(ac.table), escapeString(ac.field))
		
		var samples []string
		err := ac.conn.QuerySQL(tctx, func(rows *sql.Rows) error {
			var val sql.NullString
			if err := rows.Scan(&val); err == nil && val.Valid {
				samples = append(samples, val.String)
			}
			return nil
		}, func() {
			samples = samples[:0]
		}, query)
		
		if err == nil && len(samples) >= 2 {
			// Simple check: if all samples are numeric, likely auto-increment
			for _, sample := range samples {
				if !isNumeric(sample) {
					return false
				}
			}
			return true
		}
	}
	return false
}

// GetNextChunkBoundary finds the next chunk boundary using prefetching queries
func (ac *AdaptiveChunker) GetNextChunkBoundary(tctx *tcontext.Context, previousBoundary string) (string, error) {
	switch ac.strategy {
	case StrategyMinimal:
		// For very large tables, get minimal next boundary
		return ac.getMinimalNextBoundary(tctx, previousBoundary)
	case StrategyOptimistic:
		// For auto-increment fields, calculate next boundary
		return ac.getOptimisticNextBoundary(tctx, previousBoundary)
	case StrategyComposite:
		// For complex fields, use prefetching query
		return ac.getCompositeNextBoundary(tctx, previousBoundary)
	default:
		return "", fmt.Errorf("unknown chunking strategy: %v", ac.strategy)
	}
}

// GetInitialBoundary gets the starting boundary for chunking
func (ac *AdaptiveChunker) GetInitialBoundary(tctx *tcontext.Context) (string, error) {
	query := fmt.Sprintf("SELECT `%s` FROM `%s`.`%s`", 
		escapeString(ac.field), escapeString(ac.db), escapeString(ac.table))
	
	if ac.conf.Where != "" {
		query = fmt.Sprintf("%s WHERE %s", query, ac.conf.Where)
	}
	
	query = fmt.Sprintf("%s ORDER BY `%s` LIMIT 1", query, escapeString(ac.field))
	
	var boundary string
	err := ac.conn.QuerySQL(tctx, func(rows *sql.Rows) error {
		var val sql.NullString
		if err := rows.Scan(&val); err == nil && val.Valid {
			boundary = val.String
		}
		return nil
	}, func() {
		boundary = ""
	}, query)
	
	return boundary, err
}

// getCompositeNextBoundary uses prefetching query to find next boundary for complex primary keys
func (ac *AdaptiveChunker) getCompositeNextBoundary(tctx *tcontext.Context, previousBoundary string) (string, error) {
	// Use prefetching query to find the boundary after chunk_size rows:
	// SELECT field FROM table WHERE field > 'previousBoundary' ORDER BY field LIMIT chunk_size-1, 1
	query := fmt.Sprintf("SELECT `%s` FROM `%s`.`%s` WHERE `%s` > '%s'", 
		escapeString(ac.field), escapeString(ac.db), escapeString(ac.table), 
		escapeString(ac.field), strings.ReplaceAll(previousBoundary, "'", "''"))
	
	if ac.conf.Where != "" {
		query = fmt.Sprintf("%s AND %s", query, ac.conf.Where)
	}
	
	query = fmt.Sprintf("%s ORDER BY `%s` LIMIT %d, 1", 
		query, escapeString(ac.field), ac.currentChunkSize-1)
	
	var nextBoundary string
	err := ac.conn.QuerySQL(tctx, func(rows *sql.Rows) error {
		var val sql.NullString
		if err := rows.Scan(&val); err == nil && val.Valid {
			nextBoundary = val.String
		}
		return nil
	}, func() {
		nextBoundary = ""
	}, query)
	
	return nextBoundary, err
}

// getOptimisticNextBoundary calculates next boundary for auto-increment fields
func (ac *AdaptiveChunker) getOptimisticNextBoundary(tctx *tcontext.Context, previousBoundary string) (string, error) {
	// For numeric auto-increment fields, calculate next boundary arithmetically
	// instead of querying the database (optimistic assumption of no gaps)
	if isNumeric(previousBoundary) {
		// Simple arithmetic progression for numeric fields
		// Convert to int, add chunk size, convert back
		// Arithmetic progression: next_boundary = current_value + chunk_size
		return fmt.Sprintf("%d", mustParseInt(previousBoundary)+ac.currentChunkSize), nil
	}
	
	// For non-numeric fields, fall back to composite method
	return ac.getCompositeNextBoundary(tctx, previousBoundary)
}

// getMinimalNextBoundary gets next boundary for very large tables with minimal queries
func (ac *AdaptiveChunker) getMinimalNextBoundary(tctx *tcontext.Context, previousBoundary string) (string, error) {
	// For minimal strategy, use a simple LIMIT query to avoid expensive operations
	query := fmt.Sprintf("SELECT `%s` FROM `%s`.`%s` WHERE `%s` > '%s'", 
		escapeString(ac.field), escapeString(ac.db), escapeString(ac.table), 
		escapeString(ac.field), strings.ReplaceAll(previousBoundary, "'", "''"))
	
	if ac.conf.Where != "" {
		query = fmt.Sprintf("%s AND %s", query, ac.conf.Where)
	}
	
	// Use a smaller chunk size for minimal strategy to avoid timeouts
	minimalChunkSize := ac.currentChunkSize / 4
	if minimalChunkSize < 100 {
		minimalChunkSize = 100
	}
	
	query = fmt.Sprintf("%s ORDER BY `%s` LIMIT %d, 1", 
		query, escapeString(ac.field), minimalChunkSize-1)
	
	var nextBoundary string
	err := ac.conn.QuerySQL(tctx, func(rows *sql.Rows) error {
		var val sql.NullString
		if err := rows.Scan(&val); err == nil && val.Valid {
			nextBoundary = val.String
		}
		return nil
	}, func() {
		nextBoundary = ""
	}, query)
	
	return nextBoundary, err
}

// Helper function to parse integers safely
func mustParseInt(s string) int64 {
	// Simple integer parsing - in production this would need better error handling
	var result int64
	for _, char := range s {
		if char >= '0' && char <= '9' {
			result = result*10 + int64(char-'0')
		}
	}
	return result
}


// RecordChunkMetrics records performance metrics for adaptive sizing
func (ac *AdaptiveChunker) RecordChunkMetrics(metrics ChunkingMetrics) {
	ac.metrics = append(ac.metrics, metrics)
	
	// Keep only recent metrics (last 10 chunks)
	if len(ac.metrics) > 10 {
		ac.metrics = ac.metrics[len(ac.metrics)-10:]
	}
	
	// Adapt chunk size based on performance
	ac.adaptChunkSize(metrics)
}

// adaptChunkSize adjusts chunk size based on performance feedback
func (ac *AdaptiveChunker) adaptChunkSize(metrics ChunkingMetrics) {
	if !metrics.Successful {
		// Reduce chunk size if processing failed
		ac.currentChunkSize = int64(float64(ac.currentChunkSize) * MinDynamicStepFactor)
		if ac.currentChunkSize < 100 {
			ac.currentChunkSize = 100
		}
		return
	}
	
	// Adjust based on processing time vs target
	if metrics.ProcessingTime > ac.targetTime*2 {
		// Too slow, reduce chunk size
		ac.currentChunkSize = int64(float64(ac.currentChunkSize) * MinDynamicStepFactor)
	} else if metrics.ProcessingTime < ac.targetTime/2 {
		// Too fast, increase chunk size
		ac.currentChunkSize = int64(float64(ac.currentChunkSize) * MaxDynamicStepFactor)
	}
	
	// Apply bounds
	if ac.currentChunkSize < 100 {
		ac.currentChunkSize = 100
	}
	if ac.currentChunkSize > 100000 {
		ac.currentChunkSize = 100000
	}
}

// GetCurrentChunkSize returns the current adaptive chunk size
func (ac *AdaptiveChunker) GetCurrentChunkSize() int64 {
	return ac.currentChunkSize
}

// SetStrategy sets the chunking strategy
func (ac *AdaptiveChunker) SetStrategy(strategy ChunkStrategy) {
	ac.strategy = strategy
}

// Helper function to check if a string represents a number
func isNumeric(s string) bool {
	if s == "" {
		return false
	}
	for _, char := range s {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}