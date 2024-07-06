package godq_test

import (
	"path/filepath"
	"testing"
)

func tempFilePath(t *testing.T) string {
	t.Helper()
	tempDir := t.TempDir()
	return filepath.Join(tempDir, "queue_test.db")
}