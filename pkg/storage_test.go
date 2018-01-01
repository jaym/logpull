package logpull

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var EXAMPLE_FILES = []FileDesc{
	FileDesc{
		FilePath: "filename.txt",
		Sha256:   "sha256-0",
	},
	FileDesc{
		FilePath: "filename.txt",
		Sha256:   "sha256-1",
	},
}

func TestStorage(t *testing.T) {
	tempdir, err := ioutil.TempDir("", "logpull")
	if err != nil {
		t.Fatal(err)
	}

	store, err := NewStore(tempdir)
	require.NoError(t, err, "Could not create store")
	defer store.Close()
	defer os.RemoveAll(tempdir)

	t.Run("Test read empty feed", func(t *testing.T) {
		files, next, err := store.ReadFeed("empty", 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), next)
		assert.Equal(t, []FileDesc{}, files)
	})

	t.Run("Test append to empty feed", func(t *testing.T) {
		feed := "append-empty"
		store.appendToFeed(feed, EXAMPLE_FILES[0])
		files, next, err := store.ReadFeed(feed, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), next)

		expected := EXAMPLE_FILES[0]
		expected.Id = 1

		assert.Equal(t, []FileDesc{expected}, files)
	})

	t.Run("Test multiple append", func(t *testing.T) {
		feed := "append-existing"
		store.appendToFeed(feed, EXAMPLE_FILES[0])
		store.appendToFeed(feed, EXAMPLE_FILES[1])

		t.Run("Can read from beginning", func(t *testing.T) {
			files, next, err := store.ReadFeed(feed, 0)
			require.NoError(t, err)
			assert.Equal(t, uint64(3), next)

			expected0 := EXAMPLE_FILES[0]
			expected0.Id = 1

			expected1 := EXAMPLE_FILES[1]
			expected1.Id = 2

			assert.Equal(t, []FileDesc{expected0, expected1}, files)
		})
	})

	t.Run("Test sequence reading", func(t *testing.T) {
		feed := "seq-read"
		seq := uint64(0)

		t.Run("Empty read", func(t *testing.T) {
			files, next, err := store.ReadFeed(feed, seq)
			require.NoError(t, err)
			assert.Equal(t, []FileDesc{}, files)
			seq = next
		})

		store.appendToFeed(feed, EXAMPLE_FILES[0])

		t.Run("After adding first", func(t *testing.T) {
			files, next, err := store.ReadFeed(feed, seq)
			require.NoError(t, err)

			expected := EXAMPLE_FILES[0]
			expected.Id = 1

			assert.Equal(t, []FileDesc{expected}, files)
			seq = next
		})

		store.appendToFeed(feed, EXAMPLE_FILES[1])
		t.Run("After adding second", func(t *testing.T) {
			files, next, err := store.ReadFeed(feed, seq)
			require.NoError(t, err)

			expected := EXAMPLE_FILES[1]
			expected.Id = 2

			assert.Equal(t, []FileDesc{expected}, files)
			seq = next
		})

		store.appendToFeed(feed, EXAMPLE_FILES[0])
		t.Run("After adding fourth", func(t *testing.T) {
			files, next, err := store.ReadFeed(feed, seq)
			require.NoError(t, err)

			expected := EXAMPLE_FILES[0]
			expected.Id = 3

			assert.Equal(t, []FileDesc{expected}, files)
			seq = next
		})

		t.Run("No new appended", func(t *testing.T) {
			files, next, err := store.ReadFeed(feed, seq)
			require.NoError(t, err)
			assert.Equal(t, []FileDesc{}, files)
			seq = next
		})
		assert.Equal(t, uint64(4), seq)
	})

	t.Run("Test append file relative path", func(t *testing.T) {
		feed := "no-append-file"
		filePath := "testdata/example_file"
		err := store.AppendFileToFeed(feed, filePath)
		require.Error(t, err)
	})

	t.Run("Test append file abs path", func(t *testing.T) {
		feed := "append-file"
		filePath, err := filepath.Abs("testdata/example_file")
		require.NoError(t, err)
		store.AppendFileToFeed(feed, filePath)
		files, next, err := store.ReadFeed(feed, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), next)

		expected := FileDesc{
			Id:       1,
			Sha256:   "b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c",
			FilePath: filePath,
		}

		assert.Equal(t, []FileDesc{expected}, files)
	})
}
