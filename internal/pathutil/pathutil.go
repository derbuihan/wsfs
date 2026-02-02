// Package pathutil provides utilities for converting between FUSE paths and Databricks remote paths.
//
// The wsfs filesystem uses two path domains:
//   - fusePath: User-facing path with ".ipynb" suffix for notebooks (e.g., "/Users/test/notebook.ipynb")
//   - remotePath: Databricks API path without ".ipynb" suffix (e.g., "/Users/test/notebook")
//
// This package centralizes the conversion logic to prevent bugs from scattered transformations.
package pathutil

import "strings"

// NotebookSuffix is the file extension added to notebooks in the FUSE layer
const NotebookSuffix = ".ipynb"

// ToRemotePath converts a FUSE path to a Databricks remote path by stripping the .ipynb suffix.
// If the path doesn't have the suffix, it returns the path unchanged.
func ToRemotePath(fusePath string) string {
	return strings.TrimSuffix(fusePath, NotebookSuffix)
}

// ToFuseName converts a remote name to a FUSE-visible name.
// For notebooks, it adds the .ipynb suffix.
func ToFuseName(remoteName string, isNotebook bool) string {
	if isNotebook {
		return remoteName + NotebookSuffix
	}
	return remoteName
}

// HasNotebookSuffix checks if a path has the .ipynb suffix
func HasNotebookSuffix(path string) bool {
	return strings.HasSuffix(path, NotebookSuffix)
}
