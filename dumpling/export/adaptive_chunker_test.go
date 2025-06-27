// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/stretchr/testify/require"
)

func TestNewAdaptiveChunker(t *testing.T) {
	conf := DefaultConfig()
	chunker := NewAdaptiveChunker("test_db", "test_table", "id", conf, nil)
	
	require.Equal(t, "test_db", chunker.db)
	require.Equal(t, "test_table", chunker.table)
	require.Equal(t, "id", chunker.field)
	require.Equal(t, StrategyComposite, chunker.strategy)
	require.Equal(t, DefaultChunkTargetTime, chunker.targetTime)
	require.Equal(t, int64(DefaultStartingChunkSize), chunker.currentChunkSize)
	require.NotNil(t, chunker.metrics)
}

func TestDetermineStrategy(t *testing.T) {
	tests := []struct {
		name          string
		rowCount      int64
		field         string
		expectedStrategy ChunkStrategy
	}{
		{
			name:             "any table with string field uses composite strategy",
			rowCount:         1000,
			field:            "name",
			expectedStrategy: StrategyComposite,
		},
		{
			name:             "large table also uses composite strategy",
			rowCount:         100000000,
			field:            "email",
			expectedStrategy: StrategyComposite,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tctx := tcontext.Background()
			conf := DefaultConfig()
			chunker := NewAdaptiveChunker("test_db", "test_table", tt.field, conf, nil)
			
			strategy := chunker.DetermineStrategy(tctx, tt.rowCount)
			require.Equal(t, tt.expectedStrategy, strategy)
		})
	}
}



func TestRecordChunkMetrics(t *testing.T) {
	conf := DefaultConfig()
	chunker := NewAdaptiveChunker("test_db", "test_table", "id", conf, nil)
	
	initialSize := chunker.GetCurrentChunkSize()

	// Test successful fast chunk - should increase size
	fastMetrics := ChunkingMetrics{
		ProcessingTime: 50 * time.Millisecond, // Much faster than target (500ms)
		RowsProcessed:  1000,
		ChunkSize:      initialSize,
		Successful:     true,
	}
	chunker.RecordChunkMetrics(fastMetrics)
	
	// Chunk size should increase
	require.Greater(t, chunker.GetCurrentChunkSize(), initialSize)

	// Test slow chunk - should decrease size
	slowMetrics := ChunkingMetrics{
		ProcessingTime: 2 * time.Second, // Much slower than target (500ms)
		RowsProcessed:  1000,
		ChunkSize:      chunker.GetCurrentChunkSize(),
		Successful:     true,
	}
	chunker.RecordChunkMetrics(slowMetrics)
	
	// Chunk size should decrease
	require.Less(t, chunker.GetCurrentChunkSize(), initialSize)

	// Test failed chunk - should decrease size significantly
	prevSize := chunker.GetCurrentChunkSize()
	failedMetrics := ChunkingMetrics{
		ProcessingTime: 1 * time.Second,
		RowsProcessed:  0,
		ChunkSize:      prevSize,
		Successful:     false,
	}
	chunker.RecordChunkMetrics(failedMetrics)
	
	// Chunk size should decrease after failure
	require.Less(t, chunker.GetCurrentChunkSize(), prevSize)
}

func TestAdaptChunkSize(t *testing.T) {
	conf := DefaultConfig()
	chunker := NewAdaptiveChunker("test_db", "test_table", "id", conf, nil)
	
	initialSize := chunker.GetCurrentChunkSize()

	tests := []struct {
		name           string
		metrics        ChunkingMetrics
		expectIncrease bool
		expectDecrease bool
	}{
		{
			name: "fast processing increases size",
			metrics: ChunkingMetrics{
				ProcessingTime: 100 * time.Millisecond, // Much faster than 500ms target
				Successful:     true,
			},
			expectIncrease: true,
		},
		{
			name: "slow processing decreases size",
			metrics: ChunkingMetrics{
				ProcessingTime: 1200 * time.Millisecond, // Much slower than 500ms target
				Successful:     true,
			},
			expectDecrease: true,
		},
		{
			name: "failed processing decreases size",
			metrics: ChunkingMetrics{
				ProcessingTime: 500 * time.Millisecond,
				Successful:     false,
			},
			expectDecrease: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunker.currentChunkSize = initialSize // Reset to initial size
			
			chunker.adaptChunkSize(tt.metrics)
			
			if tt.expectIncrease {
				require.Greater(t, chunker.GetCurrentChunkSize(), initialSize)
			} else if tt.expectDecrease {
				require.Less(t, chunker.GetCurrentChunkSize(), initialSize)
			}
		})
	}
}

func TestIncrementalBoundaryDiscovery(t *testing.T) {
	tests := []struct {
		name         string
		strategy     ChunkStrategy
		setupMocks   func(sqlmock.Sqlmock)
	}{
		{
			name:     "composite strategy",
			strategy: StrategyComposite,
			setupMocks: func(mock sqlmock.Sqlmock) {
				// Initial boundary query
				mock.ExpectQuery("SELECT .* ORDER BY .* LIMIT 1").WillReturnRows(
					sqlmock.NewRows([]string{"field"}).AddRow("sample1"))
				// Next boundary query
				mock.ExpectQuery("SELECT .* WHERE .* > .* ORDER BY .* LIMIT").WillReturnRows(
					sqlmock.NewRows([]string{"field"}).AddRow("sample2"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			tt.setupMocks(mock)

			tctx := tcontext.Background()
			conn, err := db.Conn(tctx)
			require.NoError(t, err)
			baseConn := newBaseConn(conn, false, nil)

			conf := DefaultConfig()
			chunker := NewAdaptiveChunker("test_db", "test_table", "field", conf, baseConn)
			chunker.SetStrategy(tt.strategy)

			// Test incremental boundary discovery approach
			initialBoundary, err := chunker.GetInitialBoundary(tctx)
			require.NoError(t, err)
			require.NotEmpty(t, initialBoundary)
			
			// Test getting next boundary
			nextBoundary, err := chunker.GetNextChunkBoundary(tctx, initialBoundary)
			require.NoError(t, err)
		})
	}
}


func TestChunkMetricsTracking(t *testing.T) {
	conf := DefaultConfig()
	chunker := NewAdaptiveChunker("test_db", "test_table", "id", conf, nil)

	// Add more than 10 metrics to test the sliding window
	for i := 0; i < 15; i++ {
		metrics := ChunkingMetrics{
			ProcessingTime: time.Duration(i*100) * time.Millisecond,
			RowsProcessed:  int64(1000 + i*100),
			ChunkSize:      int64(1000),
			Successful:     true,
		}
		chunker.RecordChunkMetrics(metrics)
	}

	// Should keep only the last 10 metrics
	require.Equal(t, 10, len(chunker.metrics))
	
	// Verify the metrics are the most recent ones
	require.Equal(t, time.Duration(5*100)*time.Millisecond, chunker.metrics[0].ProcessingTime)
	require.Equal(t, time.Duration(14*100)*time.Millisecond, chunker.metrics[9].ProcessingTime)
}

func TestPrefetchingQueries(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Mock initial boundary
	mock.ExpectQuery("SELECT .* ORDER BY .* LIMIT 1").WillReturnRows(
		sqlmock.NewRows([]string{"field"}).AddRow("start"))
	
	// Mock next boundary query
	mock.ExpectQuery("SELECT .* WHERE .* > .* ORDER BY .* LIMIT").WillReturnRows(
		sqlmock.NewRows([]string{"field"}).AddRow("middle"))

	tctx := tcontext.Background()
	conn, err := db.Conn(tctx)
	require.NoError(t, err)
	baseConn := newBaseConn(conn, false, nil)

	conf := DefaultConfig()
	chunker := NewAdaptiveChunker("test_db", "test_table", "field", conf, baseConn)
	chunker.SetStrategy(StrategyComposite)

	// Test incremental boundary discovery using prefetching queries
	initialBoundary, err := chunker.GetInitialBoundary(tctx)
	require.NoError(t, err)
	require.Equal(t, "start", initialBoundary)

	// Get next boundary using prefetching
	nextBoundary, err := chunker.GetNextChunkBoundary(tctx, initialBoundary)
	require.NoError(t, err)
	require.Equal(t, "middle", nextBoundary)
}