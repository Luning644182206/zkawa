// Copyright (c) 2017, The zkawa Authors.
// All rights reserved.
//
// Author: Zheng Gonglin <scaugrated@gmail.com>
// Created: 2017/02/18

package zcask

import (
    "errors"
    "fmt"
    "io"
    "log"
    "os"
    "path"
)

const (
    ZDataFileSuffix         = ".zdata"
)

type ZDataFile interface {
    Size()  int64           // size of file
    Path() string           // return path
    FileId() uint64         // return fileId
    Close() error

    ReadZRecordAt(offset int64) (*ZRecord, error)
    ReadRawBytesAt(data []byte, offset int64) (int, error)
}

type ActiveDataFile struct {
    f           *FileWithBuffer
    fileId      uint64
    path        string
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

    return readZRecordAt(adf, offset)
}

func (adf *ActiveDataFile) ReadRawBytesAt(data []byte, offset int64) (int, error) {
    return adf.f.ReadAt(data, offset)
}

func (adf *ActiveDataFile) Close() error {
    if err := adf.f.Close(); err != nil {
        return err
    }
    return nil
}

type FileWithBuffer struct {
    f               *os.File
    buffer          []byte
    bufferSize      uint32      // len(buffer)
    bufferUsed      uint32      // writed used
    size            int64       // total size: f.Size() + bufferUsed
}

func NewFileWithBuffer(f *os.File, size uint32) (*FileWithBuffer, error) {
    return &FileWithBuffer {
        f:          f,
        buffer:     make([]byte, size),
        bufferSize: size,
        bufferUsed: 0,
        size:       0,
    }, nil
}

func (fwb *FileWithBuffer) Size() int64 {
    return fwb.size
}

func (fwb *FileWithBuffer) Write(data []byte) (int, error) {
    length := uint32(len(data))

    // buffer is empty and data was large, write directly.
    if fwb.bufferUsed == 0 && length >= fwb.bufferSize {
        n, err := fwb.f.Write(data)
        if err != nil {
            log.Printf("write large data directly, writed size: %d, error details: %v", n, err)
            return n, err
        }
        fwb.size += int64(n)
        return n, err
    }

    // buffer is not enough to save the whole data
    // flush buffer first, and then copy data to buffer
    if length > fwb.bufferSize - fwb.bufferUsed {
        if n, err := fwb.Flush(); err != nil {
            log.Printf("flush buffer before copy, flushed size: %d, error details: %v", n, err)
            return n, err
        }
    }

    n := copy(fwb.buffer[fwb.bufferUsed:], data)
    if uint32(n) != length {
        errMsg := fmt.Sprintf(`copy data to write buffer failed, expectd size is %d, actual size is %s`, length, n)
        return n, errors.New(errMsg)
    }

    // flush and copy succeed
    fwb.bufferUsed += uint32(n)
    fwb.size += int64(n)

    return n, nil
}

func (fwb *FileWithBuffer) ReadAt(data []byte, offset int64) (int, error) {
    fileSize := fwb.size - int64(fwb.bufferUsed)

    // data was in buffer, read from buffer
    if offset >= fileSize {
        bufferOffset := offset - fileSize
        n := copy(data, fwb.buffer[bufferOffset: fwb.bufferUsed])
        if n < len(data) {
            log.Printf("data in buffer, bufferOffset:%d, bufferUsed:%d, delta:%d, length:%d, copy:%d\n",
                bufferOffset, fwb.bufferUsed, int64(fwb.bufferUsed)-bufferOffset, len(data), n)
            return n, io.EOF
        }
        return n, nil
    }

    // data was in file, read from file
    return fwb.f.ReadAt(data, offset)
}

func (fwb *FileWithBuffer) Flush() (int, error) {
    if fwb.bufferUsed == 0 {
        return 0, nil
    }

    writed := 0
    for pos := uint32(0); pos < fwb.bufferUsed;  {
        n, err := fwb.f.Write(fwb.buffer[pos:fwb.bufferUsed])
        if err != nil && n <= 0{
            log.Printf("flush write buffer to file failed, writed %d bytes, error details: %v", n, err)
            return n, err
        }
        pos += uint32(n)
        writed += n
    }

    fwb.bufferUsed = 0

    return writed, nil
}

func (fwb *FileWithBuffer) Close() error {
    if _, err := fwb.Flush(); err != nil {
        return err
    }
    return fwb.f.Close()
}
