// Package pathutil provides notebook path and source helpers.
package pathutil

import (
	"strings"

	"github.com/databricks/databricks-sdk-go/service/workspace"
)

// NotebookFallbackSuffix is used when a notebook has no known source suffix
// or when its preferred source suffix collides with an exact workspace entry.
const NotebookFallbackSuffix = ".ipynb"

var sourceSuffixes = []struct {
	suffix   string
	language workspace.Language
}{
	{suffix: ".scala", language: workspace.LanguageScala},
	{suffix: ".sql", language: workspace.LanguageSql},
	{suffix: ".py", language: workspace.LanguagePython},
	{suffix: ".R", language: workspace.LanguageR},
}

// NotebookSourceSuffix returns the visible suffix for notebooks in source mode.
// Unknown languages do not have a source suffix and fall back to .ipynb.
func NotebookSourceSuffix(language workspace.Language) string {
	for _, candidate := range sourceSuffixes {
		if candidate.language == language {
			return candidate.suffix
		}
	}
	return ""
}

// NotebookVisibleName returns the preferred visible name for a notebook.
func NotebookVisibleName(remoteName string, language workspace.Language) string {
	if suffix := NotebookSourceSuffix(language); suffix != "" {
		return remoteName + suffix
	}
	return NotebookFallbackName(remoteName)
}

// NotebookVisiblePath returns the preferred visible path for a notebook.
func NotebookVisiblePath(remotePath string, language workspace.Language) string {
	if suffix := NotebookSourceSuffix(language); suffix != "" {
		return remotePath + suffix
	}
	return NotebookFallbackPath(remotePath)
}

// NotebookFallbackName returns the compatibility .ipynb visible name.
func NotebookFallbackName(remoteName string) string {
	return remoteName + NotebookFallbackSuffix
}

// NotebookFallbackPath returns the compatibility .ipynb visible path.
func NotebookFallbackPath(remotePath string) string {
	return remotePath + NotebookFallbackSuffix
}

// AllNotebookSourceSuffixes returns every supported source suffix (e.g. ".py", ".sql").
func AllNotebookSourceSuffixes() []string {
	suffixes := make([]string, len(sourceSuffixes))
	for i, s := range sourceSuffixes {
		suffixes[i] = s.suffix
	}
	return suffixes
}

// NotebookRemotePathFromSourcePath resolves a source-style visible path.
func NotebookRemotePathFromSourcePath(visiblePath string) (string, workspace.Language, bool) {
	for _, candidate := range sourceSuffixes {
		if strings.HasSuffix(visiblePath, candidate.suffix) {
			return strings.TrimSuffix(visiblePath, candidate.suffix), candidate.language, true
		}
	}
	return "", "", false
}

// NotebookRemotePathFromFallbackPath resolves a compatibility .ipynb path.
func NotebookRemotePathFromFallbackPath(visiblePath string) (string, bool) {
	if !strings.HasSuffix(visiblePath, NotebookFallbackSuffix) {
		return "", false
	}
	return strings.TrimSuffix(visiblePath, NotebookFallbackSuffix), true
}

// HasNotebookSourceSuffix reports whether the path ends with a source-mode suffix.
func HasNotebookSourceSuffix(path string) bool {
	_, _, ok := NotebookRemotePathFromSourcePath(path)
	return ok
}

// HasNotebookFallbackSuffix reports whether the path ends with .ipynb.
func HasNotebookFallbackSuffix(path string) bool {
	return strings.HasSuffix(path, NotebookFallbackSuffix)
}

// NotebookSourceCommentPrefix returns the comment prefix Databricks uses for source notebooks.
func NotebookSourceCommentPrefix(language workspace.Language) string {
	switch language {
	case workspace.LanguageSql:
		return "--"
	case workspace.LanguageScala:
		return "//"
	case workspace.LanguageR, workspace.LanguagePython:
		return "#"
	default:
		return "#"
	}
}

// NotebookSourceHeader returns the first line of a Databricks source notebook.
func NotebookSourceHeader(language workspace.Language) string {
	return NotebookSourceCommentPrefix(language) + " Databricks notebook source"
}

// NotebookCellDelimiter returns the line Databricks uses to separate cells.
func NotebookCellDelimiter(language workspace.Language) string {
	return NotebookSourceCommentPrefix(language) + " COMMAND ----------"
}

// collectUniquePerLanguage builds a deduplicated list by applying fn to each language.
func collectUniquePerLanguage(fn func(workspace.Language) string) []string {
	result := make([]string, 0, len(sourceSuffixes))
	seen := make(map[string]struct{}, len(sourceSuffixes))
	for _, candidate := range sourceSuffixes {
		s := fn(candidate.language)
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		result = append(result, s)
	}
	return result
}

// Pre-computed slices (the set of languages is static).
var (
	allNotebookSourceHeaders  = collectUniquePerLanguage(NotebookSourceHeader)
	allNotebookCellDelimiters = collectUniquePerLanguage(NotebookCellDelimiter)
)

// AllNotebookSourceHeaders returns every supported source header variant.
func AllNotebookSourceHeaders() []string {
	return allNotebookSourceHeaders
}

// AllNotebookCellDelimiters returns every supported cell delimiter variant.
func AllNotebookCellDelimiters() []string {
	return allNotebookCellDelimiters
}
