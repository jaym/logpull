package logpull

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var EXAMPLE_FILES = []FileDesc{
	FileDesc{
		Guid:     "guid-0",
		FileName: "filename.txt",
		Sha256:   "sha256-0",
	},
	FileDesc{
		Guid:     "guid-1",
		FileName: "filename.txt",
		Sha256:   "sha256-1",
	},
}

func TestStorage(t *testing.T) {
	tempdir, err := ioutil.TempDir("", "logpull")
	if err != nil {
		t.Fatal(err)
	}
	tempfile := path.Join(tempdir, "test.db")

	store, err := NewStore(tempfile)
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
		store.AppendToFeed(feed, EXAMPLE_FILES[0])
		files, next, err := store.ReadFeed(feed, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), next)
		assert.Equal(t, []FileDesc{EXAMPLE_FILES[0]}, files)
	})

	t.Run("Test multiple append", func(t *testing.T) {
		feed := "append-existing"
		store.AppendToFeed(feed, EXAMPLE_FILES[0])
		store.AppendToFeed(feed, EXAMPLE_FILES[1])

		t.Run("Can read from beginning", func(t *testing.T) {
			files, next, err := store.ReadFeed(feed, 0)
			require.NoError(t, err)
			assert.Equal(t, uint64(3), next)
			assert.Equal(t, []FileDesc{EXAMPLE_FILES[0], EXAMPLE_FILES[1]}, files)
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

		store.AppendToFeed(feed, EXAMPLE_FILES[0])

		t.Run("After adding first", func(t *testing.T) {
			files, next, err := store.ReadFeed(feed, seq)
			require.NoError(t, err)
			assert.Equal(t, []FileDesc{EXAMPLE_FILES[0]}, files)
			seq = next
		})

		store.AppendToFeed(feed, EXAMPLE_FILES[1])
		t.Run("After adding second", func(t *testing.T) {
			files, next, err := store.ReadFeed(feed, seq)
			require.NoError(t, err)
			assert.Equal(t, []FileDesc{EXAMPLE_FILES[1]}, files)
			seq = next
		})

		store.AppendToFeed(feed, EXAMPLE_FILES[0])
		t.Run("After adding fourth", func(t *testing.T) {
			files, next, err := store.ReadFeed(feed, seq)
			require.NoError(t, err)
			assert.Equal(t, []FileDesc{EXAMPLE_FILES[0]}, files)
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

}
