package pathutil

import "testing"

func TestToRemotePath(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "notebook with .ipynb suffix",
			input:    "/Users/test/notebook.ipynb",
			expected: "/Users/test/notebook",
		},
		{
			name:     "regular file without suffix",
			input:    "/Users/test/file.txt",
			expected: "/Users/test/file.txt",
		},
		{
			name:     "file with .ipynb in middle of name",
			input:    "/Users/test/file.ipynb.txt",
			expected: "/Users/test/file.ipynb.txt",
		},
		{
			name:     "just filename with .ipynb",
			input:    "notebook.ipynb",
			expected: "notebook",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "path ending with .ipynb directory",
			input:    "/Users/test/.ipynb",
			expected: "/Users/test/",
		},
		{
			name:     "nested notebook path",
			input:    "/Users/test/subdir/deep/notebook.ipynb",
			expected: "/Users/test/subdir/deep/notebook",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToRemotePath(tt.input)
			if result != tt.expected {
				t.Errorf("ToRemotePath(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestToFuseName(t *testing.T) {
	tests := []struct {
		name       string
		remoteName string
		isNotebook bool
		expected   string
	}{
		{
			name:       "notebook gets .ipynb suffix",
			remoteName: "notebook",
			isNotebook: true,
			expected:   "notebook.ipynb",
		},
		{
			name:       "regular file unchanged",
			remoteName: "file.txt",
			isNotebook: false,
			expected:   "file.txt",
		},
		{
			name:       "directory unchanged",
			remoteName: "subdir",
			isNotebook: false,
			expected:   "subdir",
		},
		{
			name:       "empty name notebook",
			remoteName: "",
			isNotebook: true,
			expected:   ".ipynb",
		},
		{
			name:       "empty name non-notebook",
			remoteName: "",
			isNotebook: false,
			expected:   "",
		},
		{
			name:       "notebook with special chars",
			remoteName: "my-notebook_v2",
			isNotebook: true,
			expected:   "my-notebook_v2.ipynb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToFuseName(tt.remoteName, tt.isNotebook)
			if result != tt.expected {
				t.Errorf("ToFuseName(%q, %v) = %q, want %q", tt.remoteName, tt.isNotebook, result, tt.expected)
			}
		})
	}
}

func TestHasNotebookSuffix(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{
			name:     "path with .ipynb suffix",
			path:     "/Users/test/notebook.ipynb",
			expected: true,
		},
		{
			name:     "path without suffix",
			path:     "/Users/test/file.txt",
			expected: false,
		},
		{
			name:     "path with .ipynb in middle",
			path:     "/Users/test/file.ipynb.txt",
			expected: false,
		},
		{
			name:     "just .ipynb",
			path:     ".ipynb",
			expected: true,
		},
		{
			name:     "empty string",
			path:     "",
			expected: false,
		},
		{
			name:     "similar but not exact suffix",
			path:     "/Users/test/file.IPYNB",
			expected: false, // case sensitive
		},
		{
			name:     "partial suffix",
			path:     "/Users/test/file.ipyn",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HasNotebookSuffix(tt.path)
			if result != tt.expected {
				t.Errorf("HasNotebookSuffix(%q) = %v, want %v", tt.path, result, tt.expected)
			}
		})
	}
}

// TestRoundTrip verifies that ToRemotePath and ToFuseName are inverse operations for notebooks
func TestRoundTrip(t *testing.T) {
	tests := []struct {
		remoteName string
	}{
		{"notebook"},
		{"my-notebook"},
		{"notebook_v2"},
		{"深層学習"},
	}

	for _, tt := range tests {
		t.Run(tt.remoteName, func(t *testing.T) {
			// Remote -> Fuse -> Remote should be identity
			fuseName := ToFuseName(tt.remoteName, true)
			backToRemote := ToRemotePath(fuseName)
			if backToRemote != tt.remoteName {
				t.Errorf("Round trip failed: %q -> %q -> %q", tt.remoteName, fuseName, backToRemote)
			}
		})
	}
}
