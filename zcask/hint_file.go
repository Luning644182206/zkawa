// Copyright (c) 2017, The zkawa Authors.
// All rights reserved.
//
// Author: Zheng Gonglin <scaugrated@gmail.com>
// Created: 2017/02/24

package zcask

import (
    "errors"
    "fmt"
    "os"
)

const (
    ZHintFileSuffix = ".zhint"
)

type ReadableHintFile struct {
    f           *os.File
    fileId      uint64
    path        string
    size        int64
}

type WritableHintFile struct {
    f           *FileWithBuffer
    fileId      uint64
    path        string
}

func NewReadableHintFile(fileId uint64, filePath string) (*ReadableHintFile, error) {
    var f *os.File
    var err error
    var size int64

    f, err = os.OpenFile(filePath, os.O_RDONLY, 0444)
    if err != nil {
        return nil, err
    }

    size, err = getFileSize(f)
    if err != nil {
        return nil, err
    }

    return &ReadableHintFile {
        f:      f,
        fileId: fileId,
        path:   filePath,
        size:   size,
    }, nil
}

func NewWritableHintFile(fileId uint64, filePath string, writeBufferSize uint32) (*WritableHintFile, error) {
    var f *os.File
    var err error

    f, err = os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
    if err != nil {
        return nil, err
    }

    fwb, err := NewFileWithBuffer(f, writeBufferSize)
    if err != nil {
        return nil, err
    }

    return &WritableHintFile{
        f:          fwb,
        fileId:     fileId,
        path:       filePath,
    }, nil
}

func (rhf *ReadableHintFile) Size() int64 {
    return rhf.size
}

func (whf *WritableHintFile) Size() int64 {
    return whf.f.Size()
}

func (rhf *ReadableHintFile) Path() string {
    return rhf.path
}

func (whf *WritableHintFile) Path() string {
    return whf.path
}

func (rhf *ReadableHintFile) FileId() uint64 {
    return rhf.fileId
}

func (whf *WritableHintFile) FileId() uint64 {
    return whf.fileId
}

func (rhf *ReadableHintFile) ReadZHintRecordAt(offset int64) (*ZHintRecord, error) {
    if offset > rhf.size {
        return nil, errors.New(
            fmt.Sprintf("offset exceed the size of hint file '%s'", rhf.path))
    }

    hbytes := make([]byte, ZHintRecordHeaderSize)
    _, err := rhf.f.ReadAt(hbytes, offset)
    if err != nil {
        return nil, err
    }

    header, err := decodeZHintRecordHeader(hbytes)
    if err != nil {
        return nil, err
    }

    kbytes := make([]byte, header.keySize)
    _, err = rhf.f.ReadAt(kbytes, offset + ZRecordHeaderSize)
    if err != nil {
        return nil, err
    }

    return &ZHintRecord {
        header: *header,
        key:    kbytes,
    }, nil

}

func (whf *WritableHintFile) WriteZHintRecord(key []byte, tv *TableValue) error {
    header, err := encodeZHintRecordHeader(key, tv)
    if err != nil {
        return err
    }

    writedBytes, err := whf.f.Write(header)
    if err != nil || writedBytes != ZHintRecordHeaderSize {
        return err
    }

    writedBytes, err = whf.f.Write(key)
    if err != nil || writedBytes != len(key) {
        return err
    }

    return nil
}

func (rhf *ReadableHintFile) Close() error {
    if err := rhf.f.Close(); err != nil {
        return err
    }

    return nil
}

func (whf *WritableHintFile) Close() error {
    if err := whf.f.Close(); err != nil {
        return err
    }

    return nil
}
