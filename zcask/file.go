// Copyright (c) 2017, The zkawa Authors.
// All rights reserved.
//
// Author: Zheng Gonglin <scaugrated@gmail.com>
// Created: 2017/06/02

package zcask

import (
    "errors"
    "fmt"
    "log"
    "io"
    "os"
)

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
