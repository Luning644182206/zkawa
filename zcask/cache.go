// Copyright (c) 2017, The zkawa Authors.
// All rights reserved.
//
// Author: Zheng Gonglin <scaugrated@gmail.com>
// Created: 2017/06/08

package zcask

import (
    "container/list"
    "errors"
    "fmt"
    "log"
    "sync"
)

const (
    DefaultOpenOldDataFile  = 128
    DefaultCacheOldDataFile = 128
)

type OldDataFileCache struct {
    fileCapacity    uint32
    entityCapacity  uint32

    ceList          *list.List
    dict            map[string]*list.Element
    fileCount       uint32
    entityCount     uint32
    mutex           sync.Mutex
}

func NewOldDataFileCache(fileCapacity, entityCapacity uint32) (*OldDataFileCache, error) {
    if fileCapacity == 0 {
        fileCapacity = DefaultOpenOldDataFile
    }
    if entityCapacity == 0 {
        entityCapacity = DefaultCacheOldDataFile
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
