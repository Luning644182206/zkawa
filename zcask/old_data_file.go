// Copyright (c) 2017, The zkawa Authors.
// All rights reserved.
//
// Author: Zheng Gonglin <scaugrated@gmail.com>
// Created: 2017/06/12

package zcask

import (
    "container/list"
    "errors"
    "fmt"
    "log"
    "os"
    "path/filepath"
    "strconv"
    "strings"
    "sync"
    "syscall"
)

const (
    DefaultOpenOldDataFile  = 128
    DefaultCacheOldDataFile = 128
)

type readRawBytesCallback func(*OldDataFile, []byte, int64) (int, error)

type OldDataFile struct {
    f           *os.File
    // base infomation
    fileId      uint64
    path        string
    size        int64
    // data cache
    isCached    bool
    data        []byte
    refCount    uint32
    isInCache   bool
    // how to read data
    callback    readRawBytesCallback
}

type OldDataFileCache struct {
    fileCapacity    uint32
    entityCapacity  uint32

    ceList          *list.List
    dict            map[string]*list.Element
    fileCount       uint32
    entityCount     uint32
    mutex           sync.Mutex
}

func NewOldDataFile(path string, isCached bool) (*OldDataFile, error) {
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

    var data []byte
    var cb readRawBytesCallback
    if isCached {
        stat, err := f.Stat()
        if err != nil {
            return nil, err
        }

        if stat.Size() > 0 {
            data, err = syscall.Mmap(int(f.Fd()), 0, int(stat.Size()), syscall.PROT_READ, syscall.MAP_PRIVATE)
            if err != nil {
                return nil, err
            }
        }
        cb = readOldDataFileFromMemory
    } else {
        // log.Printf("open old data file '%s' without mmap\n", path)
        cb = readOldDataFileFromDisk
    }

    return &OldDataFile {
        f:          f,
        // base infomation
        fileId:     fileId,
        path:       path,
        size:       size,
        // data cache
        isCached:   isCached,
        data:       data,
        refCount:   1,          // is one but not zero.
        isInCache:  false,
        // how to get data
        callback:   cb,
    }, nil
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
        errMsg := fmt.Sprintf("offset exceed the size of old data file[%s]", odf.path)
        return nil, errors.New(errMsg)
    }

    return readZRecordAt(odf, offset)
}

func (odf *OldDataFile) Close() (error) {
    odf.refCount--
    if odf.refCount == 0 && !odf.isInCache {
        // log.Printf("close old data file '%s' by LRU cache eliminate\n", odf.path)
        if err := odf.f.Close(); err != nil {
            return err
        }
    }
    return nil
}

func (odf *OldDataFile) ReadRawBytesAt(data []byte, offset int64) (int, error) {
    return odf.callback(odf, data, offset)
}

func readOldDataFileFromMemory(odf *OldDataFile, data []byte, offset int64) (int, error) {
    outOfSizeError := errors.New("out of old data file cache size.")
    end := offset + int64(len(data))
    if end > odf.size {
        return -1, outOfSizeError
    }
    copy(data, odf.data[offset:end])
    return len(data), nil
}

func readOldDataFileFromDisk(odf *OldDataFile, data []byte, offset int64) (int, error) {
    return odf.f.ReadAt(data, offset)
}

func NewOldDataFileCache(fileCapacity, entityCapacity uint32) (*OldDataFileCache, error) {
    if fileCapacity == 0 {
        fileCapacity = DefaultOpenOldDataFile
    }
    if entityCapacity > fileCapacity {
        entityCapacity = fileCapacity
    }
    return &OldDataFileCache {
        fileCapacity:   fileCapacity,
        entityCapacity: entityCapacity,
        ceList:         list.New(),
        dict:           make(map[string]*list.Element, 128),
        fileCount:      0,
        entityCount:    0,
        // mutex
    }, nil
}

func (odfc *OldDataFileCache) Get(filePath string) (*OldDataFile, error) {
    odfc.mutex.Lock()
    defer odfc.mutex.Unlock()

    var dataFile *OldDataFile
    var ok bool
    var e *list.Element
    var err error

    e, ok = odfc.dict[filePath]
    if !ok {
        // set isCached flag
        isCached := true
        if odfc.entityCount >= odfc.entityCapacity {
            isCached = false
        }
        // open data file
        dataFile, err = NewOldDataFile(filePath, isCached)
        if err != nil {
            errMsg := fmt.Sprintf("open old data file '%s' for cached failed, details: %v",
                filePath, err)
            return nil, errors.New(errMsg)
        }
        // update old data file cache
        e = odfc.ceList.PushFront(dataFile)
        odfc.dict[filePath] = e
        odfc.fileCount++
        if isCached {
            odfc.entityCount++
        }
        // update old data file
        dataFile.isInCache = true
        dataFile.refCount++
    } else {
        dataFile, ok = e.Value.(*OldDataFile)
        if !ok {
            log.Fatal("can't convert list element's value to old data file.")
        }
        // update old data file cache
        odfc.ceList.MoveToFront(e)
        // update old data file
        dataFile.refCount++
    }

    // eliminate the elements in list
    for uint32(odfc.ceList.Len()) > odfc.fileCapacity {
        e = odfc.ceList.Back()
        odfc.releaseElement(e)
    }

    return dataFile, nil
}

func (odfc *OldDataFileCache) Delete(filePath string) error {
    e, ok := odfc.dict[filePath]
    if !ok {
        log.Fatalf("miss '%s' cache entity, delete failed.", filePath)
    }
    return odfc.releaseElement(e)
}

func (odfc *OldDataFileCache) Release() error {
    odfc.mutex.Lock()
    defer odfc.mutex.Unlock()

    for odfc.ceList.Len() > 0 {
        e := odfc.ceList.Back()
        if err := odfc.releaseElement(e); err != nil {
            log.Fatalf("release element from cache failed, details: %v", err)
        }
    }
    return nil
}

func (odfc *OldDataFileCache) releaseElement(e *list.Element) error {
    // update old data file cache
    v := odfc.ceList.Remove(e)
    dataFile, ok := v.(*OldDataFile)
    if !ok {
        log.Fatal("can't convert list element's value to old data file.")
    }
    delete(odfc.dict, dataFile.path)
    // update old data file
    dataFile.isInCache = false

    // try to close old data file
    // if old data file's refCount is more than zero, will not close.
    err := dataFile.Close()
    if err != nil {
        errMsg := fmt.Sprintf("delete data file '%s' failed, details: %v",
            dataFile.path, err)
        return errors.New(errMsg)
    }

    return nil
}
