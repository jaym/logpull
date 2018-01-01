package logpull

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"

	"github.com/boltdb/bolt"
	"github.com/sirupsen/logrus"
)

type Store struct {
	db *bolt.DB
}

type FileDesc struct {
	Guid string // A unique string representing the file. This should be the
	// symlink file name
	FileName string // The filename to use when pulling this file
	Sha256   string // The checksum of the file
}

func NewStore(path string) (*Store, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	return &Store{db}, nil
}

func (s *Store) Close() {
	s.db.Close()
}

// Append a FileDesc to a feed
func (s *Store) AppendToFeed(feedName string, item FileDesc) error {
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