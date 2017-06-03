// Copyright (c) 2017, The zkawa Authors.
// All rights reserved.
//
// Author: Zheng Gonglin <scaugrated@gmail.com>
// Created: 2017/02/18

package zcask

import (
    "encoding/binary"
    "errors"
    "fmt"
    "hash/crc32"
    "os"
    "path"
    "path/filepath"
    "strconv"
    "strings"
)

const (
    ZDataFileSuffix                 = ".zdata"

    DataFileWriteBufferSize         = 1024 * 1024 * 4

    ZRecordHeaderSize               = 4 + 8 + 8 + 4 + 4

    ZRecordHeaderCRC32Begin         = 0
    ZRecordHeaderCRC32End           = 4

    ZRecordHeaderTimestampBegin     = 4
    ZRecordHeaderTimestampEnd       = 12

    ZRecordHeaderExpirationBegin    = 12
    ZRecordHeaderExpirationEnd      = 20

    ZRecordHeaderKeySizeBegin       = 20
    ZRecordHeaderKeySizeEnd         = 24

    ZRecordHeaderValueSizeBegin     = 24
    ZRecordHeaderValueSizeEnd       = 28
)

type ZRecordHeader struct {
    // Header: 28 bytes
    // ------------------------------------------------------
    // |4     |8         |8          |4        |4           |
    // ------------------------------------------------------
    // |crc32 |timestamp |expiration |key size | value size |
    // ------------------------------------------------------
    CRC32       uint32
    Timestamp   uint64
    Expiration  uint64
    KeySize     uint32
    ValueSize   uint32
}

type ZRecord struct {
    Header  ZRecordHeader
    Key     []byte
    Value   []byte
}

// ZDataFile save ZRecord and end with ".zdata"
type ZDataFile interface {
    Size()  int64           // size of file
    Path() string           // return path
    FileId() uint64         // return fileId
    Close() error

    ReadZRecordAt(offset int64) (*ZRecord, error)
}

type ActiveDataFile struct {
    f           *FileWithBuffer
    fileId      uint64
    path        string

    // TODO(Zheng Gonglin): write buffer
}

type OldDataFile struct {
    f           *os.File
    fileId      uint64
    path        string
    size        int64
}

// OldDataFileCache is a simple map struct
type OldDataFileCache struct {
    cache       map[string]*OldDataFile     // path -> *OldDataFile
}

func NewActiveDataFile(dataFileDirectory string, writeBufferSize uint32) (*ActiveDataFile, error) {
    unixNano := getCurrentUnixNano()
    baseFileName := fmt.Sprintf("%d%s", unixNano, ZDataFileSuffix)
    filePath := path.Join(dataFileDirectory, baseFileName)

    f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
    if err != nil {
        return nil, err
    }

    fwb, err := NewFileWithBuffer(f, writeBufferSize)
    if err != nil {
        return nil, err
    }

    return &ActiveDataFile {
        f:      fwb,
        fileId: unixNano,
        path:   filePath,
    }, nil
}

func NewOldDataFile(path string) (*OldDataFile, error) {
    f, err := os.OpenFile(path, os.O_RDONLY, 0444)
    if err != nil {
        return nil, err
    }

    baseName := filepath.Base(path)
    fileIdStr := strings.TrimSuffix(baseName, ZDataFileSuffix)
    fileId, err := strconv.ParseUint(fileIdStr, 10, 64)
    if err != nil {
        return nil, err
    }

    size, err := getFileSize(f)
    if err != nil {
        return nil, err
    }

    return &OldDataFile {
        f: f,
        fileId: fileId,
        path: path,
        size: size,
    }, nil
}

func NewOldDataFileCache() (*OldDataFileCache, error) {
    return &OldDataFileCache {
        cache: make(map[string]*OldDataFile, 64),
    }, nil
}

func (adf *ActiveDataFile) Size() int64 {
    return adf.f.Size()
}

func (adf *ActiveDataFile) Path() string {
    return adf.path
}

func (adf *ActiveDataFile) FileId() uint64 {
    return adf.fileId
}

func (adf *ActiveDataFile) WriteZRecord(key []byte, value []byte, timestamp, expiration uint64) (int64, error) {
    keySize := len(key)
    valueSize := len(value)

    header, err := encodeZRecordHeader(key, value, timestamp, expiration)
    if err != nil {
        return 0, err
    }

    offset := adf.f.Size()

    writedBytes, err := adf.f.Write(header)
    if err != nil || writedBytes != ZRecordHeaderSize {
        return -1, err
    }

    writedBytes, err = adf.f.Write(key)
    if err != nil || writedBytes != keySize {
        return -1, err
    }

    writedBytes, err = adf.f.Write(value)
    if err != nil || writedBytes != valueSize {
        return -1, err
    }

    return offset, nil
}

func (adf *ActiveDataFile) ReadZRecordAt(offset int64) (*ZRecord, error) {
    if offset >= adf.f.Size() {
        errMes := "offset exceed the size of active data file."
        return nil, errors.New(errMes)
    }

    return readZRecordAt(adf.f, offset)
}

func (adf *ActiveDataFile) Close() error {
    if err := adf.f.Close(); err != nil {
        return err
    }
    return nil
}

func (odf *OldDataFile) Size() int64 {
    return odf.size
}

func (odf *OldDataFile) Path() string {
    return odf.path
}

func (odf *OldDataFile) FileId() uint64 {
    return odf.fileId
}

func (odf *OldDataFile) ReadZRecordAt(offset int64) (*ZRecord, error) {
    if offset >= odf.size {
        return nil, errors.New(
            fmt.Sprintf("offset exceed the size of old data file[%s]", odf.path))
    }

    return readZRecordAt(odf.f, offset)
}

func (odf *OldDataFile) Close() (error) {
    return odf.f.Close()
}

func (c *OldDataFileCache) Get(path string) (*OldDataFile, error) {
    oldf, ok := c.cache[path]
    if !ok {
        oldf, err := NewOldDataFile(path)
        if err != nil {
            return nil, err
        }
        c.cache[path] = oldf
        return oldf, nil
    }
    return oldf, nil
}

func (c *OldDataFileCache) Delete(path string) error {
    oldf, ok := c.cache[path]
    if !ok {
        return nil
    }
    err := oldf.Close()
    if err != nil {
        return err
    }
    delete(c.cache, path)
    return nil
}

func (c *OldDataFileCache) Release() error {
    for _, f := range c.cache {
        f.Close()
    }
    c.cache = nil
    return nil
}

func (zr *ZRecord) Size() uint32 {
    return uint32(ZRecordHeaderSize) + zr.Header.KeySize + zr.Header.ValueSize
}

func encodeZRecordHeader(key []byte, value []byte, timestamp, expiration uint64) ([]byte, error) {
    header := make([]byte, ZRecordHeaderSize)

    keySize := len(key)
    valueSize := len(value)

    // fill header
    binary.LittleEndian.PutUint64(
        header[ZRecordHeaderTimestampBegin:ZRecordHeaderTimestampEnd],
        timestamp)
    binary.LittleEndian.PutUint64(
        header[ZRecordHeaderExpirationBegin:ZRecordHeaderExpirationEnd],
        expiration)
    binary.LittleEndian.PutUint32(
        header[ZRecordHeaderKeySizeBegin:ZRecordHeaderKeySizeEnd],
        uint32(keySize))
    binary.LittleEndian.PutUint32(
        header[ZRecordHeaderValueSizeBegin:ZRecordHeaderValueSizeEnd],
        uint32(valueSize))

    // calculate crc32
    crc := crc32.ChecksumIEEE(header[ZRecordHeaderCRC32End:])
    crc = crc32.Update(crc, crc32.IEEETable, key)
    crc = crc32.Update(crc, crc32.IEEETable, value)

    // put crc to header
    binary.LittleEndian.PutUint32(
        header[ZRecordHeaderCRC32Begin:ZRecordHeaderCRC32End],
        crc)

    return header, nil
}

func verifyZRecordHeaderCRC(header, key, value []byte) (bool, error) {
    crc := crc32.ChecksumIEEE(header[ZRecordHeaderCRC32End:])
    crc = crc32.Update(crc, crc32.IEEETable, key)
    crc = crc32.Update(crc, crc32.IEEETable, value)
    if crc != binary.LittleEndian.Uint32(header[ZRecordHeaderCRC32Begin:ZRecordHeaderCRC32End]) {
        return false, errors.New("verify zrecord was failed.")
    }
    return true, nil
}

func decodeZRecordHeader(buffer []byte) (*ZRecordHeader, error) {
    return &ZRecordHeader {
        CRC32:      binary.LittleEndian.Uint32(
                        buffer[ZRecordHeaderCRC32Begin:ZRecordHeaderCRC32End]),
        Timestamp:  binary.LittleEndian.Uint64(
                        buffer[ZRecordHeaderTimestampBegin:ZRecordHeaderTimestampEnd]),
        Expiration: binary.LittleEndian.Uint64(
                        buffer[ZRecordHeaderExpirationBegin:ZRecordHeaderExpirationEnd]),
        KeySize:    binary.LittleEndian.Uint32(
                        buffer[ZRecordHeaderKeySizeBegin:ZRecordHeaderKeySizeEnd]),
        ValueSize:  binary.LittleEndian.Uint32(
                        buffer[ZRecordHeaderValueSizeBegin:ZRecordHeaderValueSizeEnd]),
    }, nil
}

func readZRecordAt(dataFile ZCaskFile, offset int64) (*ZRecord, error) {
    hbytes := make([]byte, ZRecordHeaderSize)
    _, err := dataFile.ReadAt(hbytes, offset)
    if err != nil {
        return nil, err
    }

    header, err := decodeZRecordHeader(hbytes)
    if err != nil {
        return nil, err
    }

    kbytes := make([]byte, header.KeySize)
    _, err = dataFile.ReadAt(kbytes, offset + ZRecordHeaderSize)
    if err != nil {
        return nil, err
    }

    vbytes := make([]byte, header.ValueSize)
    _, err = dataFile.ReadAt(vbytes, offset + int64(ZRecordHeaderSize) + int64(header.KeySize))
    if err != nil {
        return nil, err
    }

    _, err = verifyZRecordHeaderCRC(hbytes, kbytes, vbytes)
    if err != nil {
        return nil, err
    }

    return &ZRecord {
        Header: *header,
        Key: kbytes,
        Value: vbytes,
    }, nil
}

func getZRecordSize(keySize, valueSize uint32) uint64 {
    return uint64(ZRecordHeaderSize + keySize + valueSize)
}
