package helpers

import (
	"log"
)

// LogError logs any error returned by f; useful when deferring Close etc.
func LogError(message string, f func() error) {
	if err := f(); err != nil {
		log.Println("message", message, "error", err)
	}
}
