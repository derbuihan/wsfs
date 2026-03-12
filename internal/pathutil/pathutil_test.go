package pathutil

import (
	"testing"

	"github.com/databricks/databricks-sdk-go/service/workspace"
)

func TestNotebookSourceSuffix(t *testing.T) {
	tests := []struct {
		language workspace.Language
		expected string
	}{
		{language: workspace.LanguagePython, expected: ".py"},
		{language: workspace.LanguageSql, expected: ".sql"},
		{language: workspace.LanguageScala, expected: ".scala"},
		{language: workspace.LanguageR, expected: ".R"},
		{language: "", expected: ""},
	}

	for _, tt := range tests {
		if got := NotebookSourceSuffix(tt.language); got != tt.expected {
			t.Fatalf("NotebookSourceSuffix(%q) = %q, want %q", tt.language, got, tt.expected)
		}
	}
}

func TestNotebookVisibleName(t *testing.T) {
	tests := []struct {
		name       string
		remoteName string
		language   workspace.Language
		expected   string
	}{
		{name: "python notebook", remoteName: "note", language: workspace.LanguagePython, expected: "note.py"},
		{name: "sql notebook", remoteName: "query", language: workspace.LanguageSql, expected: "query.sql"},
		{name: "scala notebook", remoteName: "job", language: workspace.LanguageScala, expected: "job.scala"},
		{name: "r notebook", remoteName: "report", language: workspace.LanguageR, expected: "report.R"},
		{name: "unknown notebook", remoteName: "mystery", language: "", expected: "mystery.ipynb"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NotebookVisibleName(tt.remoteName, tt.language); got != tt.expected {
				t.Fatalf("NotebookVisibleName(%q, %q) = %q, want %q", tt.remoteName, tt.language, got, tt.expected)
			}
		})
	}
}

func TestNotebookVisiblePath(t *testing.T) {
	if got := NotebookVisiblePath("/Users/test/note", workspace.LanguagePython); got != "/Users/test/note.py" {
		t.Fatalf("unexpected python visible path: %s", got)
	}
	if got := NotebookVisiblePath("/Users/test/note", ""); got != "/Users/test/note.ipynb" {
		t.Fatalf("unexpected fallback visible path: %s", got)
	}
}

func TestNotebookRemotePathFromSourcePath(t *testing.T) {
	tests := []struct {
		name        string
		visiblePath string
		expected    string
		language    workspace.Language
		ok          bool
	}{
		{name: "python", visiblePath: "/Users/test/note.py", expected: "/Users/test/note", language: workspace.LanguagePython, ok: true},
		{name: "sql", visiblePath: "/Users/test/query.sql", expected: "/Users/test/query", language: workspace.LanguageSql, ok: true},
		{name: "scala", visiblePath: "/Users/test/job.scala", expected: "/Users/test/job", language: workspace.LanguageScala, ok: true},
		{name: "r", visiblePath: "/Users/test/report.R", expected: "/Users/test/report", language: workspace.LanguageR, ok: true},
		{name: "regular file", visiblePath: "/Users/test/file.txt", expected: "", language: "", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, language, ok := NotebookRemotePathFromSourcePath(tt.visiblePath)
			if got != tt.expected || language != tt.language || ok != tt.ok {
				t.Fatalf("NotebookRemotePathFromSourcePath(%q) = (%q, %q, %v), want (%q, %q, %v)", tt.visiblePath, got, language, ok, tt.expected, tt.language, tt.ok)
			}
		})
	}
}

func TestNotebookRemotePathFromFallbackPath(t *testing.T) {
	got, ok := NotebookRemotePathFromFallbackPath("/Users/test/note.ipynb")
	if !ok {
		t.Fatal("expected fallback path to resolve")
	}
	if got != "/Users/test/note" {
		t.Fatalf("unexpected fallback remote path: %s", got)
	}

	if _, ok := NotebookRemotePathFromFallbackPath("/Users/test/note.py"); ok {
		t.Fatal("did not expect source path to resolve as fallback")
	}
}

func TestNotebookSourceSuffixHelpers(t *testing.T) {
	if !HasNotebookSourceSuffix("/Users/test/note.py") {
		t.Fatal("expected python source suffix")
	}
	if HasNotebookSourceSuffix("/Users/test/note.ipynb") {
		t.Fatal("did not expect fallback suffix to count as source suffix")
	}
	if !HasNotebookFallbackSuffix("/Users/test/note.ipynb") {
		t.Fatal("expected fallback suffix")
	}
	if HasNotebookFallbackSuffix("/Users/test/note.py") {
		t.Fatal("did not expect source suffix to count as fallback suffix")
	}
}

func TestNotebookSourceMarkers(t *testing.T) {
	tests := []struct {
		language workspace.Language
		header   string
		cell     string
	}{
		{language: workspace.LanguagePython, header: "# Databricks notebook source", cell: "# COMMAND ----------"},
		{language: workspace.LanguageSql, header: "-- Databricks notebook source", cell: "-- COMMAND ----------"},
		{language: workspace.LanguageScala, header: "// Databricks notebook source", cell: "// COMMAND ----------"},
		{language: workspace.LanguageR, header: "# Databricks notebook source", cell: "# COMMAND ----------"},
	}

	for _, tt := range tests {
		if got := NotebookSourceHeader(tt.language); got != tt.header {
			t.Fatalf("NotebookSourceHeader(%q) = %q, want %q", tt.language, got, tt.header)
		}
		if got := NotebookCellDelimiter(tt.language); got != tt.cell {
			t.Fatalf("NotebookCellDelimiter(%q) = %q, want %q", tt.language, got, tt.cell)
		}
	}
}

func TestAllNotebookSourceSuffixes(t *testing.T) {
	got := AllNotebookSourceSuffixes()
	want := []string{".scala", ".sql", ".py", ".R"}

	if len(got) != len(want) {
		t.Fatalf("AllNotebookSourceSuffixes() len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("AllNotebookSourceSuffixes()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	seen := map[string]struct{}{}
	for _, suffix := range got {
		if _, ok := seen[suffix]; ok {
			t.Fatalf("duplicate suffix %q in %v", suffix, got)
		}
		seen[suffix] = struct{}{}
	}
}

func TestAllNotebookSourceHeadersAndCellDelimiters(t *testing.T) {
	headers := AllNotebookSourceHeaders()
	wantHeaders := []string{
		"// Databricks notebook source",
		"-- Databricks notebook source",
		"# Databricks notebook source",
	}
	if len(headers) != len(wantHeaders) {
		t.Fatalf("AllNotebookSourceHeaders() len = %d, want %d (%v)", len(headers), len(wantHeaders), headers)
	}
	for i := range wantHeaders {
		if headers[i] != wantHeaders[i] {
			t.Fatalf("AllNotebookSourceHeaders()[%d] = %q, want %q", i, headers[i], wantHeaders[i])
		}
	}

	delimiters := AllNotebookCellDelimiters()
	wantDelimiters := []string{
		"// COMMAND ----------",
		"-- COMMAND ----------",
		"# COMMAND ----------",
	}
	if len(delimiters) != len(wantDelimiters) {
		t.Fatalf("AllNotebookCellDelimiters() len = %d, want %d (%v)", len(delimiters), len(wantDelimiters), delimiters)
	}
	for i := range wantDelimiters {
		if delimiters[i] != wantDelimiters[i] {
			t.Fatalf("AllNotebookCellDelimiters()[%d] = %q, want %q", i, delimiters[i], wantDelimiters[i])
		}
	}
}

func TestNotebookSourceCommentPrefixUnknownLanguageFallsBackToHash(t *testing.T) {
	if got := NotebookSourceCommentPrefix(""); got != "#" {
		t.Fatalf("NotebookSourceCommentPrefix(unknown) = %q, want #", got)
	}
}

func TestNotebookVisibleRoundTrip(t *testing.T) {
	known := []struct {
		path     string
		language workspace.Language
	}{
		{path: "/Users/test/notebook", language: workspace.LanguagePython},
		{path: "/Users/test/query", language: workspace.LanguageSql},
		{path: "/Users/test/job", language: workspace.LanguageScala},
		{path: "/Users/test/report", language: workspace.LanguageR},
	}

	for _, tt := range known {
		t.Run(string(tt.language), func(t *testing.T) {
			visible := NotebookVisiblePath(tt.path, tt.language)
			back, language, ok := NotebookRemotePathFromSourcePath(visible)
			if !ok {
				t.Fatal("expected source path round trip to resolve")
			}
			if back != tt.path || language != tt.language {
				t.Fatalf("round trip = (%q, %q), want (%q, %q)", back, language, tt.path, tt.language)
			}
		})
	}

	visible := NotebookVisiblePath("/Users/test/mystery", "")
	back, ok := NotebookRemotePathFromFallbackPath(visible)
	if !ok {
		t.Fatal("expected fallback round trip to resolve")
	}
	if back != "/Users/test/mystery" {
		t.Fatalf("unexpected fallback round trip path: %s", back)
	}
}
