// Copyright (c) 2017, The zkawa Authors.
// All rights reserved.
//
// Author: Zheng Gonglin <scaugrated@gmail.com>
// Created: 2017/02/17

package zcask

import (
    "os"
    "time"
)

func getCurrentUnixNano() uint64 {
    return uint64(time.Now().UnixNano())
}

func getCurrentUnixMicro() uint64 {
    return uint64(time.Now().UnixNano()) / 1000000
}

func getFileSize(f *os.File) (int64, error) {
    fi, err := f.Stat()
    if err != nil {
        return -1, err
    }
    return fi.Size(), nil
}

func isFile(path string) bool {
    info, err := os.Stat(path)
    if err != nil {
        return false
    }
    return !info.IsDir()
}

func isDir(path string) bool {
    info, err := os.Stat(path)
    if err != nil {
        return false
    }
    return info.IsDir()
}
