package logpull

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/boltdb/bolt"
	"github.com/sirupsen/logrus"
)

type Store struct {
	db  *bolt.DB
	dir string
}

type FileDesc struct {
	Id       uint64
	FilePath string // The filename to use when pulling this file
	Sha256   string // The checksum of the file
}

func NewStore(dir string) (*Store, error) {
	db, err := bolt.Open(path.Join(dir, "db"), 0600, nil)

	if err != nil {
		return nil, err
	}

	return &Store{
		db:  db,
		dir: dir,
	}, nil
}

func (s *Store) Close() {
	s.db.Close()
}

func (s *Store) AppendFileToFeed(feedName string, filePath string) error {
	if !filepath.IsAbs(filePath) {
		return fmt.Errorf("Provided path is not absolute")
	}

	sha, err := shasum(filePath)

	if err != nil {
		return err
	}

	return s.appendToFeed(feedName, FileDesc{
		FilePath: filePath,
		Sha256:   sha,
	})
}

func shasum(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// Append a FileDesc to a feed
func (s *Store) appendToFeed(feedName string, item FileDesc) error {
	payload, err := marshal(item)

	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketName(feedName))
		if err != nil {
			return err
		}
		id, _ := b.NextSequence()

		err = b.Put(itob(id), payload)

		if err != nil {
			return err
		}

		return nil
	})
}

func (s *Store) ReadFeed(feedName string, since uint64) ([]FileDesc, uint64, error) {
	files := []FileDesc{}
	var err error
	next := since

	s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName(feedName))

		if b == nil {
			return nil
		}

		cursor := b.Cursor()

		for k, v := cursor.Seek(itob(since)); k != nil; k, v = cursor.Next() {
			var item FileDesc
			item, err = unmarshal(v)
			if err != nil {
				logrus.WithFields(
					logrus.Fields{
						"key": k,
						"val": v,
					}).WithError(err).Error("Failed to unmarshal FileDesc")
				return err
			}
			item.Id = btoi(k)
			files = append(files, item)
		}
		next = b.Sequence()
		return nil
	})

	return files, next + 1, err
}

func bucketName(feedName string) []byte {
	return []byte("feeds/" + feedName)
}

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func btoi(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func marshal(item FileDesc) ([]byte, error) {
	buffer := new(bytes.Buffer)
	err := gob.NewEncoder(buffer).Encode(item)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func unmarshal(b []byte) (FileDesc, error) {
	item := FileDesc{}
	buffer := bytes.NewBuffer(b)
	err := gob.NewDecoder(buffer).Decode(&item)
	return item, err
}
