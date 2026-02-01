package buffer

// FileBuffer holds in-memory file data and dirty state.
type FileBuffer struct {
	Data  []byte
	Dirty bool
}
