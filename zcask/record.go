// Copyright (c) 2017, The zkawa Authors.
// All rights reserved.
//
// Author: Zheng Gonglin <scaugrated@gmail.com>
// Created: 2017/06/12

package zcask

import (
    "encoding/binary"
    "errors"
    "hash/crc32"
)

const (
    // data file record
    ZRecordHeaderSize                   = 4 + 8 + 8 + 4 + 4

    ZRecordHeaderCRC32Begin             = 0
    ZRecordHeaderCRC32End               = 4

    ZRecordHeaderTimestampBegin         = 4
    ZRecordHeaderTimestampEnd           = 12

    ZRecordHeaderExpirationBegin        = 12
    ZRecordHeaderExpirationEnd          = 20

    ZRecordHeaderKeySizeBegin           = 20
    ZRecordHeaderKeySizeEnd             = 24

    ZRecordHeaderValueSizeBegin         = 24
    ZRecordHeaderValueSizeEnd           = 28

    // hint file record
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

type ZRecordHeader struct {
    // Header: 28 bytes
    // ------------------------------------------------------
    // |4     |8         |8          |4        |4           |
    // ------------------------------------------------------
    // |crc32 |timestamp |expiration |key size | value size |
    // ------------------------------------------------------
    crc32       uint32
    timestamp   uint64
    expiration  uint64
    keySize     uint32
    valueSize   uint32
}

type ZRecord struct {
    header  ZRecordHeader
    key     []byte
    value   []byte
}

type ZHintRecordHeader struct {
    // Header: 37 bytes
    // -------------------------------------------------------------------------
    // |8            |8         |8          |8           |1          |4        |
    // -------------------------------------------------------------------------
    // |data file id |timestamp |expiration |zrecord pos |is deleted |key size |
    // -------------------------------------------------------------------------
    dataFileId      uint64
    timestamp       uint64
    expiration      uint64
    zRecordPos      int64
    isDeleted       bool
    keySize         uint32
}

type ZHintRecord struct {
    header      ZHintRecordHeader
    key         []byte
}

func (zr *ZRecord) Size() uint32 {
    return uint32(ZRecordHeaderSize) + zr.header.keySize + zr.header.valueSize
}

func (zhr *ZHintRecord) Size() uint32 {
    return uint32(ZHintRecordHeaderSize) + uint32(len(zhr.key))
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
        crc32:      binary.LittleEndian.Uint32(
                        buffer[ZRecordHeaderCRC32Begin:ZRecordHeaderCRC32End]),
        timestamp:  binary.LittleEndian.Uint64(
                        buffer[ZRecordHeaderTimestampBegin:ZRecordHeaderTimestampEnd]),
        expiration: binary.LittleEndian.Uint64(
                        buffer[ZRecordHeaderExpirationBegin:ZRecordHeaderExpirationEnd]),
        keySize:    binary.LittleEndian.Uint32(
                        buffer[ZRecordHeaderKeySizeBegin:ZRecordHeaderKeySizeEnd]),
        valueSize:  binary.LittleEndian.Uint32(
                        buffer[ZRecordHeaderValueSizeBegin:ZRecordHeaderValueSizeEnd]),
    }, nil
}

func readZRecordAt(dataFile ZDataFile, offset int64) (*ZRecord, error) {
    hbytes := make([]byte, ZRecordHeaderSize)
    _, err := dataFile.ReadRawBytesAt(hbytes, offset)
    if err != nil {
        return nil, err
    }

    header, err := decodeZRecordHeader(hbytes)
    if err != nil {
        return nil, err
    }

    kbytes := make([]byte, header.keySize)
    _, err = dataFile.ReadRawBytesAt(kbytes, offset + int64(ZRecordHeaderSize))
    if err != nil {
        return nil, err
    }

    vbytes := make([]byte, header.valueSize)
    _, err = dataFile.ReadRawBytesAt(vbytes, offset + int64(ZRecordHeaderSize) + int64(header.keySize))
    if err != nil {
        return nil, err
    }

    _, err = verifyZRecordHeaderCRC(hbytes, kbytes, vbytes)
    if err != nil {
        return nil, err
    }

    return &ZRecord {
        header: *header,
        key:    kbytes,
        value:  vbytes,
    }, nil
}

func getZRecordSize(keySize, valueSize uint32) uint64 {
    return uint64(ZRecordHeaderSize + keySize + valueSize)
}

func encodeZHintRecordHeader(key []byte, tv *TableValue) ([]byte, error) {
    header := make([]byte, ZHintRecordHeaderSize)

    // fill header
    binary.LittleEndian.PutUint64(
        header[ZHintRecordHeaderDataFileIdBegin:ZHintRecordHeaderDataFileIdEnd],
        tv.dataFileId)
    binary.LittleEndian.PutUint64(
        header[ZHintRecordHeaderTimestampBegin:ZHintRecordHeaderTimestampEnd],
        tv.timestamp)
    binary.LittleEndian.PutUint64(
        header[ZHintRecordHeaderExpirationBegin:ZHintRecordHeaderExpirationEnd],
        tv.expiration)

    if tv.isDeleted {
        header[ZHintRecordHeaderIsDeletedBegin] = 1
    } else {
        header[ZHintRecordHeaderIsDeletedBegin] = 0
    }

    binary.LittleEndian.PutUint64(
        header[ZHintRecordHeaderZRecordPosBegin:ZHintRecordHeaderZRecordPosEnd],
        uint64(tv.zRecordPos))
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
        dataFileId: binary.LittleEndian.Uint64(
                        buffer[ZHintRecordHeaderDataFileIdBegin:ZHintRecordHeaderDataFileIdEnd]),
        timestamp:  binary.LittleEndian.Uint64(
                        buffer[ZHintRecordHeaderTimestampBegin:ZHintRecordHeaderTimestampEnd]),
        zRecordPos: int64(binary.LittleEndian.Uint64(
                        buffer[ZHintRecordHeaderZRecordPosBegin:ZHintRecordHeaderZRecordPosEnd])),
        isDeleted:  isDeleted,
        expiration: binary.LittleEndian.Uint64(
                        buffer[ZHintRecordHeaderExpirationBegin:ZHintRecordHeaderExpirationEnd]),
        keySize:    binary.LittleEndian.Uint32(
                        buffer[ZHintRecordHeaderKeySizeBegin:ZHintRecordHeaderKeySizeEnd]),
    }, nil
}
