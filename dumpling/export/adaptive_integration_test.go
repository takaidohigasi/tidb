// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/stretchr/testify/require"
)

// TestAdaptiveChunkingIntegration tests the end-to-end adaptive chunking workflow
func TestAdaptiveChunkingIntegration(t *testing.T) {
	tests := []struct {
		name           string
		tableSize      int64
		fieldName      string
		expectedStrategy ChunkStrategy
		setupMocks     func(sqlmock.Sqlmock)
	}{
		{
			name:             "large table with string field uses composite strategy",
			tableSize:        200000000,
			fieldName:        "email",
			expectedStrategy: StrategyComposite,
			setupMocks: func(mock sqlmock.Sqlmock) {
				// Mock initial boundary query
				mock.ExpectQuery("SELECT .* ORDER BY .* LIMIT 1").WillReturnRows(
					sqlmock.NewRows([]string{"email"}).AddRow("aaa@example.com"))
				
				// Mock next boundary query for composite strategy
				mock.ExpectQuery("SELECT .* WHERE .* > .* ORDER BY .* LIMIT").WillReturnRows(
					sqlmock.NewRows([]string{"email"}).AddRow("zzz@example.com"))
			},
		},
		{
			name:             "small table with complex field uses composite strategy",
			fieldName:        "product_code",
			tableSize:        10000,
			expectedStrategy: StrategyComposite,
			setupMocks: func(mock sqlmock.Sqlmock) {
				// Since "product_code" doesn't contain "id", isLikelyAutoIncrement won't query the DB
				// So the first query will be GetInitialBoundary
				mock.ExpectQuery("SELECT .* ORDER BY .* LIMIT 1").WillReturnRows(
					sqlmock.NewRows([]string{"product_code"}).AddRow("ABC123"))
				
				// Mock next boundary query for composite strategy
				mock.ExpectQuery("SELECT .* WHERE .* > .* ORDER BY .* LIMIT").WillReturnRows(
					sqlmock.NewRows([]string{"product_code"}).AddRow("MNO456"))
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
			conf.Rows = 1000

			// Create and test the adaptive chunker
			chunker := NewAdaptiveChunker("test_db", "test_table", tt.fieldName, conf, baseConn)
			
			// Test strategy determination
			strategy := chunker.DetermineStrategy(tctx, tt.tableSize)
			require.Equal(t, tt.expectedStrategy, strategy)
			chunker.SetStrategy(strategy)

			// Test incremental boundary discovery chunking
			initialBoundary, err := chunker.GetInitialBoundary(tctx)
			require.NoError(t, err)
			require.NotEmpty(t, initialBoundary)
			
			// Test getting next boundary
			_, err = chunker.GetNextChunkBoundary(tctx, initialBoundary)
			require.NoError(t, err)
			// Next boundary could be empty if this is the last chunk - that's valid

			// Test performance tracking
			metrics := ChunkingMetrics{
				ProcessingTime: 200 * time.Millisecond,
				RowsProcessed:  1000,
				ChunkSize:      chunker.GetCurrentChunkSize(),
				Successful:     true,
			}
			
			chunker.RecordChunkMetrics(metrics)
			
			// Verify metrics were recorded and chunk size might have been adjusted
			require.Len(t, chunker.metrics, 1)
			require.Equal(t, metrics.ProcessingTime, chunker.metrics[0].ProcessingTime)
		})
	}
}

// TestAdaptiveChunkSizeEvolution tests how chunk sizes evolve based on performance
func TestAdaptiveChunkSizeEvolution(t *testing.T) {
	conf := DefaultConfig()
	chunker := NewAdaptiveChunker("test_db", "test_table", "id", conf, nil)
	
	initialSize := chunker.GetCurrentChunkSize()
	
	// Simulate a series of performance scenarios
	scenarios := []struct {
		name           string
		processingTime time.Duration
		successful     bool
		expectIncrease bool
		expectDecrease bool
	}{
		{
			name:           "very fast processing should increase size",
			processingTime: 50 * time.Millisecond,
			successful:     true,
			expectIncrease: true,
		},
		{
			name:           "slow processing should decrease size",
			processingTime: 1200 * time.Millisecond,
			successful:     true,
			expectDecrease: true,
		},
		{
			name:           "failed processing should decrease size",
			processingTime: 500 * time.Millisecond,
			successful:     false,
			expectDecrease: true,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			// Reset to initial size for each test
			chunker.currentChunkSize = initialSize
			
			metrics := ChunkingMetrics{
				ProcessingTime: scenario.processingTime,
				RowsProcessed:  1000,
				ChunkSize:      initialSize,
				Successful:     scenario.successful,
			}
			
			chunker.RecordChunkMetrics(metrics)
			
			if scenario.expectIncrease {
				require.Greater(t, chunker.GetCurrentChunkSize(), initialSize,
					"Chunk size should increase for fast processing")
			} else if scenario.expectDecrease {
				require.Less(t, chunker.GetCurrentChunkSize(), initialSize,
					"Chunk size should decrease for slow/failed processing")
			}
		})
	}
}

// TestPerformanceMetricsWindow tests the sliding window of performance metrics
func TestPerformanceMetricsWindow(t *testing.T) {
	conf := DefaultConfig()
	chunker := NewAdaptiveChunker("test_db", "test_table", "id", conf, nil)
	
	// Add more metrics than the window size (10)
	for i := 0; i < 15; i++ {
		metrics := ChunkingMetrics{
			ProcessingTime: time.Duration(i*100) * time.Millisecond,
			RowsProcessed:  int64(1000 + i*100),
			ChunkSize:      1000,
			Successful:     true,
			TimestampStart: time.Now().Add(-time.Duration(i) * time.Minute),
			TimestampEnd:   time.Now().Add(-time.Duration(i) * time.Minute + 200*time.Millisecond),
		}
		chunker.RecordChunkMetrics(metrics)
	}
	
	// Should only keep the last 10 metrics
	require.Equal(t, 10, len(chunker.metrics))
	
	// Verify the metrics are the most recent ones (indices 5-14)
	require.Equal(t, time.Duration(5*100)*time.Millisecond, chunker.metrics[0].ProcessingTime)
	require.Equal(t, time.Duration(14*100)*time.Millisecond, chunker.metrics[9].ProcessingTime)
}

// TestChunkingStrategyFallback tests fallback behavior between strategies
func TestChunkingStrategyFallback(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Test initial boundary query
	mock.ExpectQuery("SELECT .* ORDER BY .* LIMIT 1").WillReturnRows(
		sqlmock.NewRows([]string{"field"}).AddRow("start"))
	
	// Test next boundary with fallback
	mock.ExpectQuery("SELECT .* WHERE .* > .* ORDER BY .* LIMIT").WillReturnRows(
		sqlmock.NewRows([]string{"field"}).AddRow("next"))

	tctx := tcontext.Background()
	conn, err := db.Conn(tctx)
	require.NoError(t, err)
	baseConn := newBaseConn(conn, false, nil)

	conf := DefaultConfig()
	chunker := NewAdaptiveChunker("test_db", "test_table", "name", conf, baseConn)
	chunker.SetStrategy(StrategyComposite)

	// Test incremental boundary discovery
	initialBoundary, err := chunker.GetInitialBoundary(tctx)
	require.NoError(t, err)
	require.Equal(t, "start", initialBoundary)
	
	nextBoundary, err := chunker.GetNextChunkBoundary(tctx, initialBoundary)
	require.NoError(t, err)
	require.Equal(t, "next", nextBoundary)
}

// BenchmarkAdaptiveChunking benchmarks the performance of adaptive chunking vs traditional
func BenchmarkAdaptiveChunking(b *testing.B) {
	conf := DefaultConfig()
	
	b.Run("AdaptiveChunker", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			chunker := NewAdaptiveChunker("test_db", "test_table", "id", conf, nil)
			strategy := chunker.DetermineStrategy(tcontext.Background(), 10000)
			chunker.SetStrategy(strategy)
			
			// Simulate performance tracking
			metrics := ChunkingMetrics{
				ProcessingTime: 200 * time.Millisecond,
				RowsProcessed:  1000,
				ChunkSize:      chunker.GetCurrentChunkSize(),
				Successful:     true,
			}
			chunker.RecordChunkMetrics(metrics)
		}
	})
}

// TestErrorHandling tests various error scenarios
func TestErrorHandling(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Mock database error
	mock.ExpectQuery("SELECT MIN\\(.+\\), MAX\\(.+\\)").
		WillReturnError(fmt.Errorf("database connection lost"))

	tctx := tcontext.Background()
	conn, err := db.Conn(tctx)
	require.NoError(t, err)
	baseConn := newBaseConn(conn, false, nil)

	conf := DefaultConfig()
	chunker := NewAdaptiveChunker("test_db", "test_table", "id", conf, baseConn)
	chunker.SetStrategy(StrategyOptimistic)

	// Test error handling in incremental boundary discovery
	_, err = chunker.GetInitialBoundary(tctx)
	
	// Should handle the error gracefully
	require.Error(t, err) // Database errors should be propagated for boundary discovery
}