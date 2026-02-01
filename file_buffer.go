package main

// fileBuffer holds in-memory file data and dirty state.
type fileBuffer struct {
	data  []byte
	dirty bool
}
