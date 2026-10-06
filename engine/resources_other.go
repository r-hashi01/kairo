//go:build !unix

package engine

func fileLimit() int { return 1024 }
