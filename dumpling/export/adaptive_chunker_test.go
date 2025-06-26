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
			name:             "large table uses minimal strategy",
			rowCount:         LargeTableThreshold + 1000,
			field:            "name",
			expectedStrategy: StrategyMinimal,
		},
		{
			name:             "small table with id field uses optimistic strategy",
			rowCount:         1000,
			field:            "id",
			expectedStrategy: StrategyOptimistic,
		},
		{
			name:             "small table with auto_id field uses optimistic strategy",
			rowCount:         1000,
			field:            "auto_id",
			expectedStrategy: StrategyOptimistic,
		},
		{
			name:             "small table with string field uses composite strategy",
			rowCount:         1000,
			field:            "name",
			expectedStrategy: StrategyComposite,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			// Mock the auto-increment detection query for id fields
			if tt.field == "id" || tt.field == "auto_id" {
				rows := sqlmock.NewRows([]string{tt.field}).
					AddRow("1").
					AddRow("2").
					AddRow("3")
				mock.ExpectQuery("SELECT .* ORDER BY .* LIMIT 3").WillReturnRows(rows)
			}

			tctx := tcontext.Background()
			conn, err := db.Conn(tctx)
			require.NoError(t, err)
			baseConn := newBaseConn(conn, false, nil)

			conf := DefaultConfig()
			chunker := NewAdaptiveChunker("test_db", "test_table", tt.field, conf, baseConn)
			
			strategy := chunker.DetermineStrategy(tctx, tt.rowCount)
			require.Equal(t, tt.expectedStrategy, strategy)
		})
	}
}

func TestIsLikelyAutoIncrement(t *testing.T) {
	tests := []struct {
		name         string
		field        string
		sampleData   []string
		expected     bool
	}{
		{
			name:       "numeric id field",
			field:      "id",
			sampleData: []string{"1", "2", "3"},
			expected:   true,
		},
		{
			name:       "auto_increment field",
			field:      "auto_id",
			sampleData: []string{"100", "101", "102"},
			expected:   true,
		},
		{
			name:       "string id field",
			field:      "id",
			sampleData: []string{"abc", "def", "ghi"},
			expected:   false,
		},
		{
			name:       "non-id field",
			field:      "name",
			sampleData: []string{"1", "2", "3"},
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			// Set up mock for the sample query
			rows := sqlmock.NewRows([]string{tt.field})
			for _, sample := range tt.sampleData {
				rows.AddRow(sample)
			}
			mock.ExpectQuery("SELECT .* ORDER BY .* LIMIT 3").WillReturnRows(rows)

			tctx := tcontext.Background()
			conn, err := db.Conn(tctx)
			require.NoError(t, err)
			baseConn := newBaseConn(conn, false, nil)

			conf := DefaultConfig()
			chunker := NewAdaptiveChunker("test_db", "test_table", tt.field, conf, baseConn)
			
			result := chunker.isLikelyAutoIncrement(tctx)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestGetMinimalBoundaries(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Mock the min/max union query
	rows := sqlmock.NewRows([]string{"field"}).
		AddRow("aaa").
		AddRow("zzz")
	mock.ExpectQuery("\\(SELECT .* LIMIT 1\\) UNION ALL \\(SELECT .* DESC LIMIT 1\\)").
		WillReturnRows(rows)

	tctx := tcontext.Background()
	conn, err := db.Conn(tctx)
	require.NoError(t, err)
	baseConn := newBaseConn(conn, false, nil)

	conf := DefaultConfig()
	chunker := NewAdaptiveChunker("test_db", "test_table", "name", conf, baseConn)
	chunker.SetStrategy(StrategyMinimal)

	boundaries := chunker.getMinimalBoundaries(tctx, 5)
	require.Len(t, boundaries, 2)
	require.Equal(t, "aaa", boundaries[0])
	require.Equal(t, "zzz", boundaries[1])
}

func TestGetOptimisticBoundaries(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Mock the MIN/MAX query
	rows := sqlmock.NewRows([]string{"MIN", "MAX"}).
		AddRow("1", "1000")
	mock.ExpectQuery("SELECT MIN\\(.+\\), MAX\\(.+\\)").WillReturnRows(rows)

	tctx := tcontext.Background()
	conn, err := db.Conn(tctx)
	require.NoError(t, err)
	baseConn := newBaseConn(conn, false, nil)

	conf := DefaultConfig()
	chunker := NewAdaptiveChunker("test_db", "test_table", "id", conf, baseConn)

	boundaries := chunker.getOptimisticBoundaries(tctx, 1000, 10)
	require.Len(t, boundaries, 2)
	require.Equal(t, "1", boundaries[0])
	require.Equal(t, "1000", boundaries[1])
}

func TestTryRowNumberSampling(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Mock the ROW_NUMBER() query
	rows := sqlmock.NewRows([]string{"field"}).
		AddRow("sample1").
		AddRow("sample2").
		AddRow("sample3")
	mock.ExpectQuery("SELECT .* FROM \\(SELECT .* ROW_NUMBER\\(\\)").WillReturnRows(rows)

	tctx := tcontext.Background()
	conn, err := db.Conn(tctx)
	require.NoError(t, err)
	baseConn := newBaseConn(conn, false, nil)

	conf := DefaultConfig()
	chunker := NewAdaptiveChunker("test_db", "test_table", "name", conf, baseConn)

	boundaries := chunker.tryRowNumberSampling(tctx, 100, 5)
	require.Len(t, boundaries, 3)
	require.Equal(t, "sample1", boundaries[0])
	require.Equal(t, "sample2", boundaries[1])
	require.Equal(t, "sample3", boundaries[2])
}

func TestGetOffsetBasedBoundaries(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Mock multiple LIMIT OFFSET queries
	for i := 0; i < 3; i++ {
		rows := sqlmock.NewRows([]string{"field"}).
			AddRow(fmt.Sprintf("boundary_%d", i))
		mock.ExpectQuery("SELECT .* ORDER BY .* LIMIT \\d+, 1").WillReturnRows(rows)
	}

	tctx := tcontext.Background()
	conn, err := db.Conn(tctx)
	require.NoError(t, err)
	baseConn := newBaseConn(conn, false, nil)

	conf := DefaultConfig()
	chunker := NewAdaptiveChunker("test_db", "test_table", "name", conf, baseConn)

	boundaries := chunker.getOffsetBasedBoundaries(tctx, 1000, 3)
	require.Len(t, boundaries, 3)
	require.Equal(t, "boundary_0", boundaries[0])
	require.Equal(t, "boundary_1", boundaries[1])
	require.Equal(t, "boundary_2", boundaries[2])
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

func TestGetSampleBoundaries(t *testing.T) {
	tests := []struct {
		name         string
		strategy     ChunkStrategy
		setupMocks   func(sqlmock.Sqlmock)
		expectedLen  int
	}{
		{
			name:     "minimal strategy",
			strategy: StrategyMinimal,
			setupMocks: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"field"}).
					AddRow("min_val").
					AddRow("max_val")
				mock.ExpectQuery("\\(SELECT .* LIMIT 1\\) UNION ALL").WillReturnRows(rows)
			},
			expectedLen: 2,
		},
		{
			name:     "optimistic strategy",
			strategy: StrategyOptimistic,
			setupMocks: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"MIN", "MAX"}).
					AddRow("1", "1000")
				mock.ExpectQuery("SELECT MIN\\(.+\\), MAX\\(.+\\)").WillReturnRows(rows)
			},
			expectedLen: 2,
		},
		{
			name:     "composite strategy with ROW_NUMBER",
			strategy: StrategyComposite,
			setupMocks: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"field"}).
					AddRow("sample1").
					AddRow("sample2")
				mock.ExpectQuery("SELECT .* FROM \\(SELECT .* ROW_NUMBER\\(\\)").WillReturnRows(rows)
			},
			expectedLen: 2,
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

			boundaries, err := chunker.GetSampleBoundaries(tctx, 1000, 5)
			require.NoError(t, err)
			require.Len(t, boundaries, tt.expectedLen)
		})
	}
}

func TestIsNumeric(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"123", true},
		{"0", true},
		{"999999", true},
		{"", false},
		{"abc", false},
		{"123abc", false},
		{"12.34", false}, // Decimal not supported
		{"-123", false},  // Negative not supported
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := isNumeric(tt.input)
			require.Equal(t, tt.expected, result)
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

func TestCachingBehavior(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	// Mock for first call
	rows1 := sqlmock.NewRows([]string{"field"}).
		AddRow("boundary1").
		AddRow("boundary2")
	mock.ExpectQuery("\\(SELECT .* LIMIT 1\\) UNION ALL").WillReturnRows(rows1)

	tctx := tcontext.Background()
	conn, err := db.Conn(tctx)
	require.NoError(t, err)
	baseConn := newBaseConn(conn, false, nil)

	conf := DefaultConfig()
	chunker := NewAdaptiveChunker("test_db", "test_table", "field", conf, baseConn)
	chunker.SetStrategy(StrategyMinimal)

	// First call should hit the database
	boundaries1, err := chunker.GetSampleBoundaries(tctx, 1000, 5)
	require.NoError(t, err)
	require.Len(t, boundaries1, 2)

	// Second call should use cache (no additional mock expectations)
	boundaries2, err := chunker.GetSampleBoundaries(tctx, 1000, 5)
	require.NoError(t, err)
	require.Equal(t, boundaries1, boundaries2)

	// Verify cache is working
	require.True(t, chunker.sampleCacheValid)
	require.Equal(t, boundaries1, chunker.sampledBoundaries)
}