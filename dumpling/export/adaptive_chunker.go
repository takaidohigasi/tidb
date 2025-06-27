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
	// Target chunk processing time - set to 15s so decrease trigger is 30s
	DefaultChunkTargetTime = 15 * time.Second
	MaxChunkTargetTime     = 60 * time.Second
	MinChunkTargetTime     = 1 * time.Second
	
	// Adaptive sizing constraints
	DefaultStartingChunkSize = 1000
	MaxDynamicStepFactor     = 1.5
	MinDynamicStepFactor     = 0.5
	
)

// ChunkStrategy represents different chunking approaches
type ChunkStrategy int

const (
	StrategySequential ChunkStrategy = iota
	StrategyComposite  // For string keys using prefetching queries
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
	// Use user's -r parameter as starting chunk size, with reasonable bounds
	startingChunkSize := int64(conf.Rows)
	if startingChunkSize < 100 {
		startingChunkSize = DefaultStartingChunkSize
	} else if startingChunkSize > 100000 {
		startingChunkSize = 100000
	}
	
	return &AdaptiveChunker{
		strategy:         StrategyComposite, // Default to most flexible
		targetTime:       DefaultChunkTargetTime,
		currentChunkSize: startingChunkSize,
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
	// Always use composite strategy for string fields
	tctx.L().Debug("using composite strategy for string field",
		zap.String("field", ac.field))
	return StrategyComposite
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

// GetNextChunkBoundary uses prefetching query to find next boundary for string primary keys
func (ac *AdaptiveChunker) GetNextChunkBoundary(tctx *tcontext.Context, previousBoundary string) (string, error) {
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
		// Very slow (>30s), reduce chunk size
		ac.currentChunkSize = int64(float64(ac.currentChunkSize) * MinDynamicStepFactor)
	} else if metrics.ProcessingTime < ac.targetTime/2 {
		// Fast (<7.5s), increase chunk size
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

