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
    "log"
    "os"
)

const (
    ZHintFileSuffix                     = ".zhint"

    HintFileReadOnlyMode                = "readonly"
    HintFileAppendMode                  = "append"

    HintFileReadOnlyFlag                = os.O_RDONLY
    HintFileAppendFlag                  = os.O_CREATE|os.O_RDWR|os.O_APPEND

    HintFileReadOnlyPerm                = 0444
    HintFileAppendFlagPerm              = 0644

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

type ZHintFile interface {
    Path() string
    FileId() uint64
    Size()  int64
    //ReadZHintRecordAt(offset int64) (*ZHintRecord, error)
    WriteZHintRecord(key []byte, tv *TableValue) error
    Close() error
}

type HintFile struct {
    f           *os.File
    fileId      uint64
    path        string
    size        int64
}

func NewHintFile(fileId uint64, filePath string, mode string) (*HintFile, error) {
    var f *os.File
    var err error
    var size int64

    if mode == HintFileReadOnlyMode {
        f, err = os.OpenFile(filePath, HintFileReadOnlyFlag, HintFileReadOnlyPerm)
    } else if mode == HintFileAppendMode {
        f, err = os.OpenFile(filePath, HintFileAppendFlag, HintFileAppendFlagPerm)
    } else {
        log.Fatal("new hint file with a unknowed mode: %s", mode)
    }

    if err != nil {
        return nil, err
    }

    if mode == HintFileReadOnlyMode {
        size, err = getFileSize(f)
        if err != nil {
            return nil, err
        }
    } else {
        size = 0
    }

    return &HintFile {
        f: f,
        fileId: fileId,
        path: filePath,
        size: size,
    }, nil
}

func (hf *HintFile) Size() int64 {
    return hf.size
}

func (hf *HintFile) Path() string {
    return hf.path
}

func (hf *HintFile) FileId() uint64 {
    return hf.fileId
}

func (hf *HintFile) ReadZHintRecordAt(offset int64) (*ZHintRecord, error) {
    if offset > hf.size {
        return nil, errors.New(
            fmt.Sprintf("offset exceed the size of hint file '%s'", hf.path))
    }

    hbytes := make([]byte, ZHintRecordHeaderSize, ZHintRecordHeaderSize)
    _, err := hf.f.ReadAt(hbytes, offset)
    if err != nil {
        return nil, err
    }

    header, err := decodeZHintRecordHeader(hbytes)
    if err != nil {
        return nil, err
    }

    kbytes := make([]byte, header.KeySize, header.KeySize)
    _, err = hf.f.ReadAt(kbytes, offset + ZRecordHeaderSize)
    if err != nil {
        return nil, err
    }

    return &ZHintRecord {
        Header: *header,
        Key: kbytes,
    }, nil

}

func (hf *HintFile) WriteZHintRecord(key []byte, tv *TableValue) error {
    header, err := encodeZHintRecordHeader(key, tv)
    if err != nil {
        return err
    }

    writedBytes, err := hf.f.Write(header)
    if err != nil || writedBytes != ZHintRecordHeaderSize {
        return err
    }

    writedBytes, err = hf.f.Write(key)
    if err != nil || writedBytes != len(key) {
        return err
    }

    hf.size += int64(ZHintRecordHeaderSize + len(key))

    return nil
}

func (hf *HintFile) Close() error {
    if err := hf.f.Close(); err != nil {
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
