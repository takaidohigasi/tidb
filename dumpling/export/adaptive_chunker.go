// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

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

// AdaptiveChunker implements Spirit-inspired chunking strategies
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
	
	// Sampling cache
	sampledBoundaries []string
	lastSampleTime    time.Time
	sampleCacheValid  bool
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

// GetSampleBoundaries implements Spirit-style actual data sampling
func (ac *AdaptiveChunker) GetSampleBoundaries(tctx *tcontext.Context, count int64, targetChunks int64) ([]string, error) {
	// Use cached boundaries if still valid
	if ac.sampleCacheValid && time.Since(ac.lastSampleTime) < 5*time.Minute {
		return ac.sampledBoundaries, nil
	}
	
	var boundaries []string
	
	switch ac.strategy {
	case StrategyMinimal:
		// For very large tables, get just a few safe boundaries
		boundaries = ac.getMinimalBoundaries(tctx, targetChunks)
	case StrategyOptimistic:
		// For auto-increment fields, use min/max approach
		boundaries = ac.getOptimisticBoundaries(tctx, count, targetChunks)
	case StrategyComposite:
		// For complex fields, use actual data sampling
		boundaries = ac.getCompositeBoundaries(tctx, count, targetChunks)
	default:
		return nil, fmt.Errorf("unknown chunking strategy: %v", ac.strategy)
	}
	
	// Cache the results
	ac.sampledBoundaries = boundaries
	ac.lastSampleTime = time.Now()
	ac.sampleCacheValid = true
	
	return boundaries, nil
}

// getMinimalBoundaries gets safe boundaries for very large tables
func (ac *AdaptiveChunker) getMinimalBoundaries(tctx *tcontext.Context, targetChunks int64) []string {
	// For minimal strategy, just get first and last values to create a few safe chunks
	query := fmt.Sprintf("(SELECT `%s` FROM `%s`.`%s` ORDER BY `%s` LIMIT 1) UNION ALL (SELECT `%s` FROM `%s`.`%s` ORDER BY `%s` DESC LIMIT 1)",
		escapeString(ac.field), escapeString(ac.db), escapeString(ac.table), escapeString(ac.field),
		escapeString(ac.field), escapeString(ac.db), escapeString(ac.table), escapeString(ac.field))
	
	var boundaries []string
	err := ac.conn.QuerySQL(tctx, func(rows *sql.Rows) error {
		var val sql.NullString
		if err := rows.Scan(&val); err == nil && val.Valid {
			boundaries = append(boundaries, val.String)
		}
		return nil
	}, func() {
		boundaries = boundaries[:0]
	}, query)
	
	if err != nil {
		tctx.L().Warn("failed to get minimal boundaries", zap.Error(err))
		return []string{}
	}
	
	return boundaries
}

// getOptimisticBoundaries gets boundaries for auto-increment style fields
func (ac *AdaptiveChunker) getOptimisticBoundaries(tctx *tcontext.Context, count int64, targetChunks int64) []string {
	// Get min and max values
	query := fmt.Sprintf("SELECT MIN(`%s`), MAX(`%s`) FROM `%s`.`%s`",
		escapeString(ac.field), escapeString(ac.field), escapeString(ac.db), escapeString(ac.table))
	
	var boundaries []string
	err := ac.conn.QuerySQL(tctx, func(rows *sql.Rows) error {
		var minVal, maxVal sql.NullString
		err := rows.Scan(&minVal, &maxVal)
		if err == nil && minVal.Valid && maxVal.Valid {
			boundaries = append(boundaries, minVal.String, maxVal.String)
		}
		return err
	}, func() {
		boundaries = boundaries[:0]
	}, query)
	
	if err != nil {
		tctx.L().Warn("failed to get min/max for optimistic chunking", zap.Error(err))
		return []string{}
	}
	
	return boundaries
}

// getCompositeBoundaries implements Spirit-style composite chunking with actual data sampling
func (ac *AdaptiveChunker) getCompositeBoundaries(tctx *tcontext.Context, count int64, targetChunks int64) []string {
	// Calculate sample interval
	interval := count / targetChunks
	if interval < 1 {
		interval = 1
	}
	
	// Try ROW_NUMBER() first for even distribution (MySQL 8.0+)
	boundaries := ac.tryRowNumberSampling(tctx, interval, targetChunks)
	if len(boundaries) > 0 {
		return boundaries
	}
	
	// Fallback to offset-based sampling for older MySQL versions
	tctx.L().Info("ROW_NUMBER() not available, using offset-based sampling")
	return ac.getOffsetBasedBoundaries(tctx, count, targetChunks)
}

// tryRowNumberSampling attempts to use ROW_NUMBER() for even sampling
func (ac *AdaptiveChunker) tryRowNumberSampling(tctx *tcontext.Context, interval int64, targetChunks int64) []string {
	query := fmt.Sprintf(
		"SELECT `%s` FROM (SELECT `%s`, ROW_NUMBER() OVER (ORDER BY `%s`) as rn FROM `%s`.`%s`",
		escapeString(ac.field), escapeString(ac.field), escapeString(ac.field), 
		escapeString(ac.db), escapeString(ac.table))
	
	if ac.conf.Where != "" {
		query = fmt.Sprintf("%s WHERE %s", query, ac.conf.Where)
	}
	
	query = fmt.Sprintf("%s) t WHERE MOD(rn, %d) = 0 ORDER BY `%s` LIMIT %d",
		query, interval, escapeString(ac.field), targetChunks+1)
	
	var boundaries []string
	err := ac.conn.QuerySQL(tctx, func(rows *sql.Rows) error {
		var val sql.NullString
		if err := rows.Scan(&val); err == nil && val.Valid {
			boundaries = append(boundaries, val.String)
		}
		return nil
	}, func() {
		boundaries = boundaries[:0]
	}, query)
	
	if err != nil {
		tctx.L().Debug("ROW_NUMBER() sampling failed", zap.Error(err))
		return []string{}
	}
	
	return boundaries
}

// getOffsetBasedBoundaries uses LIMIT OFFSET for sampling (compatible with older MySQL)
func (ac *AdaptiveChunker) getOffsetBasedBoundaries(tctx *tcontext.Context, count int64, targetChunks int64) []string {
	var boundaries []string
	interval := count / targetChunks
	
	// Collect boundaries using LIMIT OFFSET
	for i := int64(0); i < targetChunks; i++ {
		offset := i * interval
		query := fmt.Sprintf("SELECT `%s` FROM `%s`.`%s`", 
			escapeString(ac.field), escapeString(ac.db), escapeString(ac.table))
		
		if ac.conf.Where != "" {
			query = fmt.Sprintf("%s WHERE %s", query, ac.conf.Where)
		}
		
		query = fmt.Sprintf("%s ORDER BY `%s` LIMIT %d, 1", 
			query, escapeString(ac.field), offset)
		
		err := ac.conn.QuerySQL(tctx, func(rows *sql.Rows) error {
			var val sql.NullString
			if err := rows.Scan(&val); err == nil && val.Valid {
				boundaries = append(boundaries, val.String)
			}
			return nil
		}, func() {}, query)
		
		if err != nil {
			tctx.L().Warn("offset-based sampling failed", 
				zap.Error(err), zap.Int64("offset", offset))
			break
		}
	}
	
	return boundaries
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
	ac.sampleCacheValid = false // Invalidate cache when strategy changes
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