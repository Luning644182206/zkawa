// Copyright (c) 2017, The zkawa Authors.
// All rights reserved.
//
// Author: Zheng Gonglin <scaugrated@gmail.com>
// Created: 2017/02/24

package zcask

import (
    "encoding/binary"
    "errors"
    "fmt"
    "os"
)

const (
    ZHintFileSuffix                     = ".zhint"

    ZHintRecordHeaderSize               = 37

    ZHintRecordHeaderDataFileIdBegin    = 0
    ZHintRecordHeaderDataFileIdEnd      = 8

    ZHintRecordHeaderTimestampBegin     = 8
    ZHintRecordHeaderTimestampEnd       = 16

    ZHintRecordHeaderExpirationBegin    = 16
    ZHintRecordHeaderExpirationEnd      = 24

    ZHintRecordHeaderZRecordPosBegin    = 24
    ZHintRecordHeaderZRecordPosEnd      = 32

    ZHintRecordHeaderIsDeletedBegin     = 32
    ZHintRecordHeaderIsDeletedEnd       = 33

    ZHintRecordHeaderKeySizeBegin       = 33
    ZHintRecordHeaderKeySizeEnd         = 37
)

type ZHintRecordHeader struct {
    // Header: 37 bytes
    // -------------------------------------------------------------------------
    // |8            |8         |8          |8           |1          |4        |
    // -------------------------------------------------------------------------
    // |data file id |timestamp |expiration |zrecord pos |is deleted |key size |
    // -------------------------------------------------------------------------
    DataFileId      uint64
    Timestamp       uint64
    Expiration      uint64
    ZRecordPos      int64
    IsDeleted       bool
    KeySize         uint32
}

type ZHintRecord struct {
    Header      ZHintRecordHeader
    Key         []byte
}

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

    kbytes := make([]byte, header.KeySize)
    _, err = rhf.f.ReadAt(kbytes, offset + ZRecordHeaderSize)
    if err != nil {
        return nil, err
    }

    return &ZHintRecord {
        Header: *header,
        Key: kbytes,
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

func (zhr *ZHintRecord) Size() uint32 {
    return uint32(ZHintRecordHeaderSize) + uint32(len(zhr.Key))
}

func encodeZHintRecordHeader(key []byte, tv *TableValue) ([]byte, error) {
    header := make([]byte, ZHintRecordHeaderSize)

    // fill header
    binary.LittleEndian.PutUint64(
        header[ZHintRecordHeaderDataFileIdBegin:ZHintRecordHeaderDataFileIdEnd],
        tv.DataFileId)
    binary.LittleEndian.PutUint64(
        header[ZHintRecordHeaderTimestampBegin:ZHintRecordHeaderTimestampEnd],
        tv.Timestamp)
    binary.LittleEndian.PutUint64(
        header[ZHintRecordHeaderExpirationBegin:ZHintRecordHeaderExpirationEnd],
        tv.Expiration)

    if tv.IsDeleted {
        header[ZHintRecordHeaderIsDeletedBegin] = 1
    } else {
        header[ZHintRecordHeaderIsDeletedBegin] = 0
    }

    binary.LittleEndian.PutUint64(
        header[ZHintRecordHeaderZRecordPosBegin:ZHintRecordHeaderZRecordPosEnd],
        uint64(tv.ZRecordPos))
    binary.LittleEndian.PutUint32(
        header[ZHintRecordHeaderKeySizeBegin:ZHintRecordHeaderKeySizeEnd],
        uint32(len(key)))

    return header, nil
}


func decodeZHintRecordHeader(buffer []byte) (*ZHintRecordHeader, error) {
    var isDeleted bool
    if buffer[ZHintRecordHeaderIsDeletedBegin] == 1 {
        isDeleted = true
    } else {
        isDeleted = false
    }

    return &ZHintRecordHeader {
        DataFileId: binary.LittleEndian.Uint64(
                        buffer[ZHintRecordHeaderDataFileIdBegin:ZHintRecordHeaderDataFileIdEnd]),
        Timestamp:  binary.LittleEndian.Uint64(
                        buffer[ZHintRecordHeaderTimestampBegin:ZHintRecordHeaderTimestampEnd]),
        ZRecordPos: int64(binary.LittleEndian.Uint64(
                        buffer[ZHintRecordHeaderZRecordPosBegin:ZHintRecordHeaderZRecordPosEnd])),
        IsDeleted:  isDeleted,
        Expiration: binary.LittleEndian.Uint64(
                        buffer[ZHintRecordHeaderExpirationBegin:ZHintRecordHeaderExpirationEnd]),
        KeySize:    binary.LittleEndian.Uint32(
                        buffer[ZHintRecordHeaderKeySizeBegin:ZHintRecordHeaderKeySizeEnd]),
    }, nil
}
