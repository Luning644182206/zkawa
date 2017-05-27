// Copyright (c) 2017, The zkawa Authors.
// All rights reserved.
//
// Author: Zheng Gonglin <scaugrated@gmail.com>
// Created: 2017/02/17

package zcask

import (
    "fmt"
    "errors"
    "io/ioutil"
    "log"
    "os"
    "path"
    "path/filepath"
    "strconv"
    "strings"
    "sync"
)

const (
    // 'initial': before 'Start'
    // 'running': after 'Start', before 'ShutDown'
    // 'closed': after 'ShutDown'
    ZCaskStateInitial = "initial"
    ZCaskStateRunning = "running"
    ZCaskStateStopped = "stopped"
)

var (
    errorKeyNotExisted  = errors.New("key was not existed")
    errorKeyExpired     = errors.New("key was expired")
    errorZCaskRunning   = errors.New("zcask is running, don't call zcask.Start again")
    errorZCaskStopped   = errors.New("zcask was stopped, don't support restart")
)

func init() {
    log.SetFlags(log.Ldate|log.Ltime|log.Lshortfile)
}

type Option struct {
    DataFileDirectory   string
    MinDataFileId       uint64              // id is unix nano
    MaxDataFileId       uint64
    MaxKeySize          uint32
    MaxValueSize        uint32
    MaxDataFileSize     int64
    IsLoadOldDataFile   bool
}

type ZCask struct {
    state               string
    option              *Option
    activeDataFile      *ActiveDataFile
    fileCache           *OldDataFileCache
    table               *HashTable
    activeTable         *HashTable
    rwMutex             sync.RWMutex
    loadMutex           sync.RWMutex
}

func NewZCask(opt Option) (*ZCask, error) {
    if !isDir(opt.DataFileDirectory) {
        errMsg := fmt.Sprintf("'%s' is not a directory", opt.DataFileDirectory)
        return nil, errors.New(errMsg)
    }

    t, err := NewHashTable()
    if err != nil {
        return nil, err
    }

    at, err := NewHashTable()
    if err != nil {
        return nil, err
    }

    fc, err := NewOldDataFileCache()
    if err != nil {
        return nil, err
    }

    return &ZCask{
        state:          ZCaskStateInitial,
        option:         &opt,
        activeDataFile: nil,    // new active data file in zcask.Start
        activeTable:    at,
        fileCache:      fc,
        table:          t,
        // rwMutex
        // loadMutex
    }, nil
}

func (z *ZCask) Start() error {
    z.rwMutex.Lock()
    defer z.rwMutex.Unlock()

    if z.state == ZCaskStateRunning {
        log.Println(errorZCaskRunning)
        return errorZCaskRunning
    } else if z.state == ZCaskStateStopped {
        // TODO(Zheng Gonglin): support restart
        log.Println(errorZCaskStopped)
        return errorZCaskStopped
    }

    if z.option.IsLoadOldDataFile {
        err := z.Load()
        if err != nil {
            log.Printf("load data file failed, details: %v", err)
            return err
        }
    }

    af, err := NewActiveDataFile(z.option.DataFileDirectory)
    if err != nil {
        log.Printf("new active data file failed, details: %v", err)
        return err
    }
    z.activeDataFile = af
    z.state = ZCaskStateRunning

    return nil
}

func (z *ZCask) ShutDown() error {
    z.rwMutex.Lock()
    defer z.rwMutex.Unlock()

    if err := z.freezeActiveDataFile(); err != nil {
        log.Printf("freeze active data file failed, details: %v", err)
        return err
    }

    if err := z.fileCache.Release(); err != nil {
        log.Printf("release file cache failed, details: %v", err)
        return err
    }

    if err := z.table.Release(); err != nil {
        log.Printf("release key table failed, details: %v", err)
        return err
    }

    z.state          = ZCaskStateStopped
    z.activeDataFile = nil
    z.fileCache      = nil
    z.table          = nil
    z.activeTable    = nil
    z.option         = nil

    return nil
}

func (z *ZCask) Load() error {
    fis, err := ioutil.ReadDir(z.option.DataFileDirectory)
    if err != nil {
        log.Fatalf("list data file directory failed, details: %v", err)
    }
    fileIds := make([]uint64, 0, 128)
    for _, fi := range fis {
        fpath := path.Join(z.option.DataFileDirectory, fi.Name())
        if !z.isDataFilePath(fpath) {
            continue
        }
        fid, err := z.parseDataFileIdByPath(fpath)
        if err != nil {
            log.Fatalf("'%s' endswith '%s' but not a invalid data file", fpath, ZDataFileSuffix)
        }
        fileIds = append(fileIds, fid)
    }

    for _, fid := range fileIds {
        // TODO(Zheng Gonglin): use goroutine to load old data file concurrently
        err = z.load(fid)
        if err != nil {
            log.Fatalf("load data file<%d> failed", fid)
        }
    }

    return nil
}

func (z *ZCask) Merge() error {
    z.rwMutex.Lock()
    fis, err := ioutil.ReadDir(z.option.DataFileDirectory)
    if err != nil {
        z.rwMutex.Unlock()
        log.Fatalf("list data file directory, details: %v", err)
    }
    fileIds := make([]uint64, 0, 128)
    for _, fi := range fis {
        fpath := path.Join(z.option.DataFileDirectory, fi.Name())
        if !z.isOldDataFilePath(fpath) {
            continue
        }
        fid, err := z.parseDataFileIdByPath(fpath)
        if err != nil {
            log.Fatalf("'%s' endswith '%s' but not a invalid data file", fpath, ZDataFileSuffix)
        }
        fileIds = append(fileIds, fid)
    }
    z.rwMutex.Unlock()

    for _, fid := range fileIds {
        // Lock rwMutex for each file
        err := z.mergeFromDataFile(fid)
        if err != nil {
            log.Fatalf("merge file<%d> failed", fid)
        }
    }

    return nil
}

func (z *ZCask) Get(key string) ([]byte, error) {
    z.rwMutex.RLock()
    defer z.rwMutex.RUnlock()
    // operate timestamp
    timestamp := getCurrentUnixNano()

    tv, err := z.table.Get(key, timestamp)
    if err != nil {
        return nil, err
    }

    dataFilePath := z.getDataFilePathById(tv.DataFileId)

    if !z.isDataFilePath(dataFilePath) {
        errMes := fmt.Sprintf("data file %s not exists, can't get value from it.", dataFilePath)
        return nil, errors.New(errMes)
    }

    var dataFile ZDataFile
    if z.isOldDataFilePath(dataFilePath) {
        oldf, err := z.getOldDataFile(dataFilePath)
        if err != nil {
            log.Fatalf("get old data file '%s' failed, details: %v", dataFilePath, err)
        }
        dataFile = oldf
    } else {
        dataFile = z.activeDataFile
    }

    record, err := dataFile.ReadZRecordAt(tv.ZRecordPos)
    if err != nil {
        log.Fatalf("read zrecord at old data file '%s' failed, details: %v", dataFilePath, err)
        return nil, err
    }

    return record.Value, nil
}

func (z *ZCask) Set(key string, value []byte, expiration uint64) error {
    z.rwMutex.Lock()
    defer z.rwMutex.Unlock()
    // operate timestamp
    timestamp := getCurrentUnixNano()

    return z.setZRecord(key, value, timestamp, expiration, /*isDelete = */false)
}

func (z *ZCask) Delete(key string) error {
    z.rwMutex.Lock()
    defer z.rwMutex.Unlock()
    // operate timestamp
    timestamp := getCurrentUnixNano()

    emptyValue := make([]byte, 0, 0)
    return z.setZRecord(key, emptyValue, timestamp, /*expiration = */0, /*isDelete = */true)
}

func (z *ZCask) Keys() ([]string, error) {
    z.rwMutex.RLock()
    defer z.rwMutex.RUnlock()

    return z.table.Keys(), nil
}

func (z *ZCask) mergeFromDataFile(fid uint64) error {
    dataFilePath := z.getDataFilePathById(fid)
    hintFilePath := z.getHintFilePathById(fid)

    z.rwMutex.Lock()
    defer z.rwMutex.Unlock()
    // operate timestamp
    timestamp := getCurrentUnixNano()

    var err error
    odf, err := z.getOldDataFile(dataFilePath)
    if err != nil {
        return err
    }

    size := odf.Size()
    var zr *ZRecord
    for offset := int64(0); offset < size; offset += int64(zr.Size()) {
        zr, err = odf.ReadZRecordAt(offset)
        if err != nil {
            return err
        }

        key := string(zr.Key)
        tv, err := z.table.Get(key, timestamp)
        if err == errorKeyNotExisted {
            continue
        } else if err == errorKeyExpired {
            z.table.Delete(key)
            continue
        }

        if zr.Header.Timestamp < tv.Timestamp {
            // old record, pass
            continue
        }

        // valid zrecord, rewrite this record to active data file.
        err = z.setZRecord(key, zr.Value, tv.Timestamp, tv.Expiration, /*isDelete = */false)
        if err != nil {
            return err
        }
    }

    z.removeMergedFile(dataFilePath, hintFilePath)

    return nil
}



func (z *ZCask) load(fileId uint64) error {
    dataFilePath := z.getDataFilePathById(fileId)
    hintFilePath := z.getHintFilePathById(fileId)

    var err error = nil
    if z.isHintFilePath(hintFilePath) {
        err = z.loadFromHintFile(fileId, hintFilePath)
    } else if z.isOldDataFilePath(dataFilePath) {
        err = z.loadFromDataFile(fileId, dataFilePath)
    } else {
        err = errors.New("data and hint file<%d> was not existed.")
    }

    if err != nil {
        return err
    }

    // delete keys which with IsDeleted
    if err := z.table.RemoveInvalidKeys(); err != nil {
        return err
    }

    return nil
}

func (z *ZCask) loadSingleZRecord(odf *OldDataFile, offset int64, timestamp uint64) (*ZRecord, error) {
    zr, err := odf.ReadZRecordAt(offset)
    if err != nil {
        return nil, err
    }

    key := string(zr.Key)
    tv, err := z.table.GetIncludeExpired(key)

    // older operate, pass
    if err == nil && zr.Header.Timestamp < tv.Timestamp {
        return zr, nil
    }

    newtv := TableValue {
        DataFileId: odf.FileId(),
        Timestamp:  zr.Header.Timestamp,
        ZRecordPos: offset,
        Expiration: zr.Header.Expiration,
        IsDeleted:  false,
    }

    if zr.Header.ValueSize == 0 {
        // is a delete record, set IsDeleted
        newtv.IsDeleted = true
    } else if zr.Header.Expiration > 0 && zr.Header.Expiration <= timestamp {
        // record was expired, ignore, set IsDeleted
        newtv.IsDeleted = true
    }

    z.table.Set(key, &newtv)

    return zr, nil
}

func (z *ZCask) loadFromHintFile(fileId uint64, filePath string) error {
    // operate time
    timestamp := getCurrentUnixNano()

    var err error

    hf, err := NewHintFile(fileId, filePath, HintFileReadOnlyMode)
    if err != nil {
        return err
    }
    defer hf.Close()

    dataFilePath := z.getDataFilePathById(fileId)
    odf, err := z.getOldDataFile(dataFilePath)
    if err != nil {
        return err
    }

    fileSize := hf.Size()
    var zhr *ZHintRecord
    for  offset := int64(0); offset < fileSize; offset += int64(zhr.Size()) {
        zhr, err = hf.ReadZHintRecordAt(offset)
        if err != nil {
            return err
        }

        _, err := z.loadSingleZRecord(odf, zhr.Header.ZRecordPos, timestamp)
        if err != nil {
            return err
        }
    }

    return nil
}

func (z *ZCask) loadFromDataFile(fileId uint64, filePath string) error {
    // operate time
    timestamp := getCurrentUnixNano()

    var err error
    odf, err := NewOldDataFile(filePath)
    if err != nil {
        return err
    }

    fileSize := odf.Size()
    var zr *ZRecord
    for  offset := int64(0); offset < fileSize; offset += int64(zr.Size()) {
        zr, err = z.loadSingleZRecord(odf, offset, timestamp)
        if err != nil {
            return err
        }
    }

    return nil
}



func (z *ZCask) setZRecord(key string, value []byte, timestamp, expiration uint64, isDelete bool) error {
    keySize := uint32(len(key))
    if keySize == 0 || keySize > z.option.MaxKeySize {
        return errors.New("key size is invalid.")
    }

    valueSize := uint32(len(value))
    if valueSize > z.option.MaxValueSize || (!isDelete && valueSize == 0) {
        return errors.New("value size is invalid.")
    }

    recordSize := getZRecordSize(keySize, valueSize)

    if isDelete {
        _, err := z.table.Get(key, timestamp)
        if err == errorKeyNotExisted {
            // don't delete a key more than once.
            return err
        }
        // if err was equal to nil, is a normal key, just delete it
        // if err was equal to ErrKeyExpired, is a expired key,
        // still set a delete record to data file and remove from hash table.
    }

    if int64(recordSize) + z.activeDataFile.Size() > z.option.MaxDataFileSize {
        if err := z.freezeActiveDataFile(); err != nil {
            log.Fatal("freeze active data file failed, details: %v", err)
        }
        if err := z.renewActiveDataFile(); err != nil {
            log.Fatal("renew active data file failed, details: %v", err)
        }
    }

    offset, err := z.activeDataFile.WriteZRecord([]byte(key), value, timestamp, expiration)
    if err != nil {
        return err
    }

    if isDelete {
        if err = z.table.Delete(key); err != nil {
            return err
        }
        return nil
    }

    fileId := z.activeDataFile.FileId()

    tv := TableValue {
        DataFileId: fileId,
        ZRecordPos: offset,
        Timestamp:  timestamp,
        Expiration: expiration,
        IsDeleted:  false,
    }
    z.table.Set(key, &tv)
    z.activeTable.Set(key, &tv)

    return nil
}

func (z *ZCask) freezeActiveDataFile() error {
    // close active data file
    if err := z.activeDataFile.Close(); err != nil {
        return nil
    }
    // make hint file
    if err := z.makeHintFile(); err != nil {
        return err
    }
    // release active data file
    if err := z.activeTable.Release(); err != nil {
        return err
    }

    return nil
}

func (z *ZCask) renewActiveDataFile() error {
    // renew a active data file
    newaf, err := NewActiveDataFile(z.option.DataFileDirectory)
    if err != nil {
        return err
    }
    z.activeDataFile = newaf

    // reset active hash table
    newat, err := NewHashTable()
    if err != nil {
        return err
    }
    z.activeTable = newat

    return nil
}

func (z *ZCask) makeHintFile() error {
    fileId := z.activeDataFile.FileId()
    filePath := z.getHintFilePathById(fileId)

    hf, err := NewHintFile(fileId, filePath, HintFileAppendMode)
    if err != nil {
        return err
    }

    var zhf ZHintFile = hf
    defer zhf.Close()
    keys := z.activeTable.Keys()
    for _, key := range keys {
        tv, err := z.activeTable.GetIncludeExpired(key)
        if err != nil {
            return err
        }
        if err := zhf.WriteZHintRecord([]byte(key), tv); err != nil {
            return err
        }
    }
    if err := zhf.Close(); err != nil {
        return err
    }

    return nil
}

func (z *ZCask) removeMergedFile(dataFilePath, hintFilePath string) error {
    if err := z.fileCache.Delete(dataFilePath); err != nil {
        return err
    }

    if err := os.Remove(dataFilePath); err != nil {
        return err
    }

    if err := os.Remove(hintFilePath); err != nil {
        return err
    }

    return nil
}

func (z *ZCask) isDataFilePath(path string) bool {
    if !isFile(path) {
        return false
    }
    return strings.HasSuffix(path, ZDataFileSuffix)
}

func (z *ZCask) isHintFilePath(path string) bool {
    if !isFile(path) {
        return false
    }

    return strings.HasSuffix(path, ZHintFileSuffix)
}

func (z *ZCask) isOldDataFilePath(path string) bool {
    if !z.isDataFilePath(path) {
        return false
    }

    if z.activeDataFile == nil {
        return true
    }

    return !(z.activeDataFile.path == path)
}

func (z *ZCask) getDataFilePathById(id uint64) string {
    n := strconv.FormatUint(id, 10)
    return path.Join(z.option.DataFileDirectory, n + ZDataFileSuffix)
}

func (z *ZCask) getHintFilePathById(id uint64) string {
    n := strconv.FormatUint(id, 10)
    return path.Join(z.option.DataFileDirectory, n + ZHintFileSuffix)
}

func (z *ZCask) parseDataFileIdByPath(path string) (uint64, error) {
    base := strings.TrimSuffix(filepath.Base(path), ZDataFileSuffix)
    return strconv.ParseUint(base, 10, 64)
}

func (z *ZCask) parseHintFileIdByPath(path string) (uint64, error) {
    base := strings.TrimSuffix(filepath.Base(path), ZHintFileSuffix)
    return strconv.ParseUint(base, 10, 64)
}

func (z *ZCask) getOldDataFile(path string) (*OldDataFile, error) {
    return z.fileCache.Get(path)
}
