// Copyright (c) 2016, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltutils // import "resenje.org/boltutils"

import (
	"fmt"

	"github.com/coreos/bbolt"
)

type NotFoundError struct {
	Key string
}

func NewNotFoundError(key string) *NotFoundError { return &NotFoundError{Key: key} }

func (e *NotFoundError) Error() string { return fmt.Sprintf("key not found %q", e.Key) }

func IsNotFoundError(err error) (yes bool) {
	_, yes = err.(*NotFoundError)
	return
}

type ExistsError struct {
	Key string
}

func NewExistsError(key string) *ExistsError { return &ExistsError{Key: key} }

func (e *ExistsError) Error() string { return fmt.Sprintf("key exists %q", e.Key) }

func IsExistsError(err error) (yes bool) {
	_, yes = err.(*ExistsError)
	return
}

// DeepGet retrieves the key named as the last element of the elements
// arguments in nested buckets named as previous elements.
func DeepGet(tx *bolt.Tx, elements ...[]byte) (data []byte) {
	length := len(elements)
	if length < 2 {
		return nil
	}
	path := elements[0]
	bucket := tx.Bucket(elements[0])
	if bucket == nil {
		return
	}
	for i := 1; i < length-1; i++ {
		path = append(path, []byte(", ")...)
		path = append(path, elements[i]...)
		bucket = bucket.Bucket(elements[i])
		if bucket == nil {
			return
		}
	}
	return bucket.Get(elements[length-1])
}

// DeepPut saves the last element of elements arguments under the
// key named by the second to last element, and all previous elements
// will be created as buckets if any of them do not exist.
// With overwrite argument set to false, this function will return
// ErrExists if the element already exists.
// Return value new will be true if element is put for the first time.
func DeepPut(tx *bolt.Tx, overwrite bool, elements ...[]byte) (new bool, err error) {
	length := len(elements)
	if length < 3 {
		return false, fmt.Errorf("insufficient number of elements %d < 3", length)
	}
	path := elements[0]
	bucket, err := tx.CreateBucketIfNotExists(elements[0])
	if err != nil {
		return false, fmt.Errorf("bucket create %s: %s", elements[0], err)
	}
	for i := 1; i < length-2; i++ {
		path = append(path, []byte(", ")...)
		path = append(path, elements[i]...)
		bucket, err = bucket.CreateBucketIfNotExists(elements[i])
		if err != nil {
			return false, fmt.Errorf("bucket create %s: %s", path, err)
		}
	}
	new = bucket.Get(elements[length-2]) == nil
	if !overwrite && !new {
		return false, NewExistsError(string(path) + ", " + string(elements[length-2]))
	}
	if err = bucket.Put(elements[length-2], elements[length-1]); err != nil {
		return new, fmt.Errorf("bucket %s put %s: %s", path, elements[length-2], err)
	}
	return new, nil
}

// DeepDelete deletes the key named as the last element of the elements
// arguments in nested buckets named as previous elements.
// With argument set to true, this function will return ErrNotFound
// if the element is not deleted.
func DeepDelete(tx *bolt.Tx, ensure bool, elements ...[]byte) (err error) {
	length := len(elements)
	if length < 2 {
		return fmt.Errorf("insufficient number of elements %d < 2", length)
	}
	path := elements[0]
	bucket := tx.Bucket(elements[0])
	if bucket == nil {
		if ensure {
			return NewNotFoundError(string(path))
		}
		return nil
	}
	for i := 1; i < length-1; i++ {
		path = append(path, []byte(", ")...)
		path = append(path, elements[i]...)
		bucket = bucket.Bucket(elements[i])
		if bucket == nil {
			if ensure {
				return NewNotFoundError(string(path))
			}
			return nil
		}
	}
	if ensure && bucket.Get(elements[length-1]) == nil {
		return NewNotFoundError(string(path) + ", " + string(elements[length-1]))
	}
	if err = bucket.Delete(elements[length-1]); err != nil {
		return fmt.Errorf("bucket %s delete %s: %s", path, elements[length-1], err)
	}
	return
}
