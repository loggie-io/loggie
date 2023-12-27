package util

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGlob(t *testing.T) {
	os.RemoveAll("test_glob")
	os.MkdirAll("test_glob/foo/bar/zoo", 0755)
	files := []string{
		"test_glob/111.txt",
		"test_glob/222.txt",
		"test_glob/foo/222.txt",
		"test_glob/foo/bar/333.txt",
		"test_glob/foo/bar/zoo/122.txt",
		"test_glob/foo/bar/zoo/444.txt",
	}
	for _, fp := range files {
		os.Create(fp)
	}

	var err error
	var fps []string

	fps, err = Glob("test_glob/*.txt", true, nil, nil)
	require.Nil(t, err, "err should be nothing")
	require.Equal(t, []string{"test_glob/111.txt", "test_glob/222.txt"}, fps)

	fps, err = Glob("test_glob/*/*.txt", true, nil, nil)
	require.Nil(t, err, "err should be nothing")
	require.Equal(t, []string{"test_glob/foo/222.txt"}, fps)

	fps, err = Glob("test_glob/**/*.txt", true, nil, nil)
	require.Nil(t, err, "err should be nothing")
	require.Equal(t, files, fps)

	fps, err = Glob("test_glob/**/*.txt", true, nil, nil)
	require.Nil(t, err, "err should be nothing")
	require.Equal(t, files, fps)

	fps, err = Glob("test_glob/**/*2*.txt", true, nil, nil)
	require.Nil(t, err, "err should be nothing")
	require.Equal(t, []string{"test_glob/222.txt", "test_glob/foo/222.txt", "test_glob/foo/bar/zoo/122.txt"}, fps)

	dirCb := func(fp string) error {
		if filepath.Base(fp) == "zoo" {
			return filepath.SkipDir
		}
		return nil
	}
	var fps2 []string
	fileCb := func(fp string) error {
		fps2 = append(fps2, fp)
		return nil
	}
	fps, err = Glob("test_glob/**/*2*.txt", true, dirCb, fileCb)
	require.Nil(t, err, "err should be nothing")
	require.Empty(t, fps)
	require.Equal(t, []string{"test_glob/222.txt", "test_glob/foo/222.txt"}, fps2)

	os.RemoveAll("test_glob")
}
