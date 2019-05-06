// Copyright (c) 2016, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltutils // import "resenje.org/boltutils"

import (
	"fmt"
	"strings"

	bolt "go.etcd.io/bbolt"
)

// NotFoundError is returned by DeepDelete if ensure options is true.
type NotFoundError struct {
	Key string
}

// NewNotFoundError returns a new instance of NotFoundError.
func NewNotFoundError(key string) *NotFoundError { return &NotFoundError{Key: key} }

func (e *NotFoundError) Error() string { return fmt.Sprintf("key not found %q", e.Key) }

// IsNotFoundError returns true if provided error is of NotFoundError type.
func IsNotFoundError(err error) (yes bool) {
	_, yes = err.(*NotFoundError)
	return
}

// ExistsError is retuned by DeepPut if overwrite option is set to false.
type ExistsError struct {
	Key string
}

// NewExistsError returns a new instance of ExistsError.
func NewExistsError(key string) *ExistsError { return &ExistsError{Key: key} }

func (e *ExistsError) Error() string { return fmt.Sprintf("key exists %q", e.Key) }

// IsExistsError returns true if provided error is of ExistsError type.
func IsExistsError(err error) (yes bool) {
	_, yes = err.(*ExistsError)
	return
}

// DeepBucket retrieves bucket named as the last element of the elements
// arguments in nested buckets named as previous elements.
func DeepBucket(tx *bolt.Tx, elements ...[]byte) (bucket *bolt.Bucket) {
	length := len(elements)
	if length < 1 {
		return nil
	}
	bucket = tx.Bucket(elements[0])
	if bucket == nil {
		return nil
	}
	for i := 1; i < length; i++ {
		bucket = bucket.Bucket(elements[i])
		if bucket == nil {
			return nil
		}
	}
	return bucket
}

// DeepGet retrieves the key named as the last element of the elements
// arguments in nested buckets named as previous elements.
func DeepGet(tx *bolt.Tx, elements ...[]byte) (data []byte) {
	length := len(elements)
	if length < 2 {
		return nil
	}
	bucket := DeepBucket(tx, elements[:length-1]...)
	if bucket == nil {
		return nil
	}
	return bucket.Get(elements[length-1])
}

// DeepCreateBucketIfNotExists creates nested buckets with names
// of the elements arguments.
func DeepCreateBucketIfNotExists(tx *bolt.Tx, elements ...[]byte) (bucket *bolt.Bucket, err error) {
	length := len(elements)
	if length < 1 {
		return nil, fmt.Errorf("insufficient number of elements %d < 1", length)
	}
	bucket, err = tx.CreateBucketIfNotExists(elements[0])
	if err != nil {
		return nil, fmt.Errorf("bucket create %s: %s", elements[0], err)
	}
	for i := 1; i < length; i++ {
		bucket, err = bucket.CreateBucketIfNotExists(elements[i])
		if err != nil {
			return nil, fmt.Errorf("bucket create %s: %s", path(elements[:i+1]...), err)
		}
	}
	return bucket, nil
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
	bucket, err := DeepCreateBucketIfNotExists(tx, elements[:length-2]...)
	if err != nil {
		return false, err
	}
	new = bucket.Get(elements[length-2]) == nil
	if !overwrite && !new {
		return false, NewExistsError(path(elements[:length-1]...))
	}
	if err = bucket.Put(elements[length-2], elements[length-1]); err != nil {
		return new, fmt.Errorf("bucket %s put %s: %s", path(elements[:length-2]...), elements[length-1], err)
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
	bucket := tx.Bucket(elements[0])
	if bucket == nil {
		if ensure {
			return NewNotFoundError(string(elements[0]))
		}
		return nil
	}
	for i := 1; i < length-1; i++ {
		bucket = bucket.Bucket(elements[i])
		if bucket == nil {
			if ensure {
				return NewNotFoundError(path(elements[:i+1]...))
			}
			return nil
		}
	}
	if ensure && bucket.Get(elements[length-1]) == nil {
		return NewNotFoundError(path(elements[:length-1]...))
	}
	if err = bucket.Delete(elements[length-1]); err != nil {
		return fmt.Errorf("bucket %s delete %s: %s", path(elements[:length-1]...), elements[length-1], err)
	}
	return
}

// DeepDeleteBucket deletes bucket named as the last element of the elements
// arguments in nested buckets named as previous elements.
// With argument set to true, this function will return ErrNotFound
// if the element is not deleted.
func DeepDeleteBucket(tx *bolt.Tx, ensure bool, elements ...[]byte) (err error) {
	length := len(elements)
	if length < 1 {
		return fmt.Errorf("insufficient number of elements %d < 1", length)
	}
	bucket := tx.Bucket(elements[0])
	if bucket == nil {
		if ensure {
			return NewNotFoundError(string(elements[0]))
		}
		return nil
	}
	if len(elements) == 1 {
		if err = tx.DeleteBucket(elements[0]); err != nil {
			if err == bolt.ErrBucketNotFound {
				if ensure {
					return NewNotFoundError(string(elements[0]))
				}
				return nil
			}
			return fmt.Errorf("bucket %s delete: %s", string(elements[0]), err)
		}
		return nil
	}
	for i := 1; i < length-1; i++ {
		bucket = bucket.Bucket(elements[i])
		if bucket == nil {
			if ensure {
				return NewNotFoundError(path(elements[:i+1]...))
			}
			return nil
		}
	}
	if err = bucket.DeleteBucket(elements[length-1]); err != nil {
		if err == bolt.ErrBucketNotFound {
			if ensure {
				return NewNotFoundError(path(elements[:length-1]...))
			}
			return nil
		}
		return fmt.Errorf("bucket %s delete %s: %s", path(elements[:length-1]...), elements[length-1], err)
	}
	return nil
}

// path returns comma delimited string with provided elements used in error
// messages.
func path(elements ...[]byte) (p string) {
	var b strings.Builder
	b.Write(elements[0])
	for _, e := range elements[1:] {
		b.Write([]byte(", "))
		b.Write(e)
	}
	return b.String()
}
