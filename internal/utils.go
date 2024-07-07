package internal

import (
	"fmt"
	"sync/atomic"
)

var queueCounter uint64

func GetUniqueTableName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, atomic.AddUint64(&queueCounter, 1))
}
