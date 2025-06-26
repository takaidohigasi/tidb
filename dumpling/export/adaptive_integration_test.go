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
			name:             "large table with string field uses minimal strategy",
			tableSize:        LargeTableThreshold * 2,
			fieldName:        "email",
			expectedStrategy: StrategyMinimal,
			setupMocks: func(mock sqlmock.Sqlmock) {
				// Mock the count estimation
				mock.ExpectQuery("SELECT.*COUNT").WillReturnRows(
					sqlmock.NewRows([]string{"count"}).AddRow(LargeTableThreshold * 2))
				
				// Mock minimal boundaries query
				mock.ExpectQuery("\\(SELECT .* LIMIT 1\\) UNION ALL").WillReturnRows(
					sqlmock.NewRows([]string{"email"}).
						AddRow("aaa@example.com").
						AddRow("zzz@example.com"))
			},
		},
		{
			name:             "medium table with ID field uses optimistic strategy",
			fieldName:        "user_id",
			tableSize:        50000,
			expectedStrategy: StrategyOptimistic,
			setupMocks: func(mock sqlmock.Sqlmock) {
				// Mock the count estimation
				mock.ExpectQuery("SELECT.*COUNT").WillReturnRows(
					sqlmock.NewRows([]string{"count"}).AddRow(50000))
				
				// Mock auto-increment detection
				mock.ExpectQuery("SELECT .* ORDER BY .* LIMIT 3").WillReturnRows(
					sqlmock.NewRows([]string{"user_id"}).
						AddRow("1").AddRow("2").AddRow("3"))
				
				// Mock min/max query for optimistic strategy
				mock.ExpectQuery("SELECT MIN\\(.+\\), MAX\\(.+\\)").WillReturnRows(
					sqlmock.NewRows([]string{"MIN", "MAX"}).AddRow("1", "50000"))
			},
		},
		{
			name:             "small table with complex field uses composite strategy",
			fieldName:        "product_code",
			tableSize:        10000,
			expectedStrategy: StrategyComposite,
			setupMocks: func(mock sqlmock.Sqlmock) {
				// Mock the count estimation
				mock.ExpectQuery("SELECT.*COUNT").WillReturnRows(
					sqlmock.NewRows([]string{"count"}).AddRow(10000))
				
				// Mock auto-increment detection (returns non-numeric)
				mock.ExpectQuery("SELECT .* ORDER BY .* LIMIT 3").WillReturnRows(
					sqlmock.NewRows([]string{"product_code"}).
						AddRow("ABC123").AddRow("DEF456").AddRow("GHI789"))
				
				// Mock ROW_NUMBER sampling for composite strategy
				mock.ExpectQuery("SELECT .* FROM \\(SELECT .* ROW_NUMBER\\(\\)").WillReturnRows(
					sqlmock.NewRows([]string{"product_code"}).
						AddRow("ABC123").
						AddRow("MNO456").
						AddRow("XYZ789"))
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

			// Test boundary sampling
			targetChunks := tt.tableSize / chunker.GetCurrentChunkSize()
			if targetChunks == 0 {
				targetChunks = 1
			}

			boundaries, err := chunker.GetSampleBoundaries(tctx, tt.tableSize, targetChunks)
			require.NoError(t, err)
			require.NotEmpty(t, boundaries)

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

	// Set up sequential fallback scenarios
	// 1. ROW_NUMBER fails
	mock.ExpectQuery("SELECT .* FROM \\(SELECT .* ROW_NUMBER\\(\\)").
		WillReturnError(fmt.Errorf("ROW_NUMBER not supported"))
	
	// 2. Fallback to offset-based sampling
	for i := 0; i < 3; i++ {
		mock.ExpectQuery("SELECT .* ORDER BY .* LIMIT \\d+, 1").WillReturnRows(
			sqlmock.NewRows([]string{"field"}).AddRow(fmt.Sprintf("fallback_%d", i)))
	}

	tctx := tcontext.Background()
	conn, err := db.Conn(tctx)
	require.NoError(t, err)
	baseConn := newBaseConn(conn, false, nil)

	conf := DefaultConfig()
	chunker := NewAdaptiveChunker("test_db", "test_table", "name", conf, baseConn)
	chunker.SetStrategy(StrategyComposite)

	boundaries, err := chunker.GetSampleBoundaries(tctx, 1000, 3)
	require.NoError(t, err)
	require.Len(t, boundaries, 3)
	
	// Verify fallback boundaries
	for i, boundary := range boundaries {
		require.Equal(t, fmt.Sprintf("fallback_%d", i), boundary)
	}
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

	boundaries, err := chunker.GetSampleBoundaries(tctx, 1000, 5)
	
	// Should handle the error gracefully and return empty boundaries
	require.NoError(t, err) // Our implementation should not propagate DB errors for sampling
	require.Empty(t, boundaries)
}