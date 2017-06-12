// Copyright (c) 2017, The zkawa Authors.
// All rights reserved.
//
// Author: Zheng Gonglin <scaugrated@gmail.com>
// Created: 2017/05/26

package zcask

import (
    "log"
    "math/rand"
    "os"
    "sort"
    "testing"
    "time"
)

const (
    RandomCharSetForZCaskTesting = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    LengthOfRandomCharSetForZCaskTesting = len(RandomCharSetForZCaskTesting)
)

var (
    operationsNumForBenchmark       int

    randomGetNumForBenchmark        int
    randomSetNumForBenchmark        int

    keysForBenchmark                []string
    valuesForBenchmark              [][]byte
    keySizeForBenchmark             int
    valueSizeForBenchmark           int

    dataFileDirectoryForBenchmark   string
    zForBenchmark                   *ZCask
)

func init() {
    log.SetFlags(log.Ldate|log.Ltime|log.Lshortfile)
}

func randomBytes(length int) []byte {
    ret := make([]byte, 0, length)
    for i := 0; i < length; i++ {
        idx := rand.Intn(LengthOfRandomCharSetForZCaskTesting)
        ret = append(ret, RandomCharSetForZCaskTesting[idx])
    }
    return ret
}

func randomString(length int) string {
    return string(randomBytes(length))
}

func assertEqualStringSlice(xss, yss []string, t *testing.T) {
    if len(xss) != len(yss) {
        t.Fatalf("string slice length different: %d != %d\n", len(xss), len(yss))
    }
    for idx, s := range xss {
        if s != yss[idx] {
            t.Fatalf("index of [%d], '%s' != '%s'\n", idx, s, yss[idx])
        }
    }
}

func assertEqualByteSlice(xbs, ybs []byte, t *testing.T) {
    if len(xbs) != len(ybs) {
        t.Fatalf("byte slice length different: %d != %d\n", len(xbs), len(ybs))
    }
    for idx, b := range xbs {
        if b != ybs[idx] {
            t.Fatalf("index of [%d], '%s' != '%s'\n", idx, b, ybs[idx])
        }
    }
}

func TestZCaskComprehensive(t *testing.T) {
    var testDataDirectory  string
    var setTimes           int
    var randomKeys         []string
    var randomValues       [][]byte
    // init test data.
    testDataDirectory = "./test_data_for_TestZCaskComprehensive"
    setTimes = 100000
    randomKeys = make([]string, 0, setTimes)
    randomValues = make([][]byte, 0, setTimes)

    if err := os.RemoveAll(testDataDirectory); err != nil {
        // TODO(Zheng Gonglin): LOG FATAL
        t.Log(err)
    }
    if err := os.Mkdir(testDataDirectory, 0700); err != nil {
        // TODO(Zheng Gonglin): LOG FATAL
        t.Log(err)
    }

    for i := 0; i < setTimes; i++ {
        ksize := rand.Intn(22) + 10
        vsize := rand.Intn(54) + 10
        randomKeys = append(randomKeys, randomString(ksize))
        randomValues = append(randomValues, randomBytes(vsize))
    }

    // zcask configuration
    opt := Option {
        dataFileDirectory: testDataDirectory,
        minDataFileId: 0,
        maxDataFileId: 12345678901234567890,
        maxKeySize: 1024,
        maxValueSize: 1024,
        maxDataFileSize: 32 << 20,
        writeBufferSize: 4 << 20,
        isLoadOldDataFile: false,
        maxOpenOldDataFile: 128,
        maxCacheOldDataFile: 4,
    }

    z, err := NewZCask(opt)
    err = z.Start()
    if err != nil {
        t.Fatalf("zcask start failed, details: %v", err)
    }

    dict := make(map[string][]byte, setTimes)

    // testing zcask.Set
    for i := 0; i < setTimes; i++ {
        dict[randomKeys[i]] = randomValues[i]
        err := z.Set(randomKeys[i],
                     randomValues[i], 0)
        if err != nil {
            t.Fatal("zcask set <%s, %s> failed, details: %v",
                randomKeys[i],
                string(randomValues[i]),
                err)
        }
    }

    // testing zcask.Keys
    keys, err := z.Keys()
    if err != nil {
        t.Fatalf("zcask.Keys failed, details: %v", err)
    }
    sort.Strings(keys)
    expectKeys := make([]string, setTimes)
    copy(expectKeys, randomKeys)
    sort.Strings(expectKeys)
    assertEqualStringSlice(keys, expectKeys, t)

    // testing zcask.Get
    for _, key := range expectKeys {
        v, err := z.Get(key)
        if err != nil {
            t.Fatalf("get key<%s> failed, details: %v", key, err)
        }
        assertEqualByteSlice(v, dict[key], t)
    }

    // testing Merge
    modifySize := setTimes / 3
    deleteSize := setTimes / 3
    modifyKeys := expectKeys[:modifySize]
    deleteKeys := expectKeys[setTimes-deleteSize:]
    expectKeys = expectKeys[:setTimes-deleteSize]
    for _, key := range modifyKeys {
        v := randomBytes(32)
        err := z.Set(key, v, 0)
        if err != nil {
            t.Fatal("zcask set <%s, %s> failed, details: %v", key, string(v), err)
        }
        dict[key] = v
    }
    for _, key := range deleteKeys {
        err := z.Delete(key)
        if err != nil {
            t.Fatal("zcask delete <%s> failed, details: %v", key, err)
        }
        delete(dict, key)
    }
    err = z.Merge()
    if err != nil {
        t.Fatal("zcask Merge failed, details: %v", err)
    }
    for _, key := range expectKeys {
        v, err := z.Get(key)
        if err != nil {
            t.Fatalf("get key<%s> failed, details: %v", key, err)
        }
        assertEqualByteSlice(v, dict[key], t)
    }

    // testing ShutDown
    if err = z.ShutDown(); err != nil {
        t.Fatal("zcask shutdown failed, details: %p", err)
    }

    // testing Load
    opt.isLoadOldDataFile = true
    z, err = NewZCask(opt)
    err = z.Start()
    if err != nil {
        t.Fatalf("zcask start failed, details: %v", err)
    }
    for _, key := range expectKeys {
        v, err := z.Get(key)
        if err != nil {
            t.Fatalf("get key<%s> failed, details: %v", err)
        }
        assertEqualByteSlice(v, dict[key], t)
    }

    // testing Delete
    for _, key := range expectKeys {
        err = z.Delete(key)
        if err != nil {
            t.Fatalf("delete key<%s> failed, details: %v", err)
        }
    }
    for _, key := range expectKeys {
        _, err := z.Get(key)
        if err == nil {
            t.Fatalf("still can get key<%s>, error", )
        }
    }

    // testing ShutDown
    if err = z.ShutDown(); err != nil {
        t.Fatal("zcask shutdown failed, details: %p", err)
    }
}

func benchmarkRandomSet(t *testing.T) {
    begin := time.Now()
    j := 0
    for i := 0; i < operationsNumForBenchmark; i++ {
        if err := zForBenchmark.Set(keysForBenchmark[j], valuesForBenchmark[j], 0); err != nil {
            log.Fatalf("Set Record[key:%s] failed, err=%s", keysForBenchmark[j], err)
        }
        j++
        if j == len(keysForBenchmark) {
            j = 0
        }
    }
    end := time.Now()
    d := end.Sub(begin)
    log.Printf("%d set operation[key: %dB, value: %dB] in %fs\n", operationsNumForBenchmark, keySizeForBenchmark, valueSizeForBenchmark, d.Seconds())
    log.Printf("average %f qps\n", float64(operationsNumForBenchmark)/d.Seconds())
    writeMB := int64(operationsNumForBenchmark) * int64(valueSizeForBenchmark) / 1e6
    log.Printf("average %f MB/s\n", float64(writeMB)/d.Seconds())
    log.Printf("average %f micros/op\n", d.Seconds()*1e6/float64(operationsNumForBenchmark))
}

func benchmarkRandomGet(t *testing.T) {
    begin := time.Now()
    for i := 0; i < operationsNumForBenchmark; i++ {
        r := rand.Intn(len(keysForBenchmark))
        _, err := zForBenchmark.Get(keysForBenchmark[r])
        if err != nil {
            t.Fatalf("Get Record[key:%s] failed", keysForBenchmark[r])
        }
        //assertEqualByteSlice(v, valuesForBenchmark[r], t)
    }
    end := time.Now()
    d := end.Sub(begin)
    log.Printf("%d get operation in %fs\n", operationsNumForBenchmark, d.Seconds())
    log.Printf("average %f qps\n", float64(operationsNumForBenchmark)/d.Seconds())
    log.Printf("average %f micros/op\n", d.Seconds()*1e6/float64(operationsNumForBenchmark))
}

func benchmarkRandomSetWhenMerge(t *testing.T) {
    done := make(chan bool, 1)
    go func() {
        zForBenchmark.Merge()
        done <- true
    }()
    benchmarkRandomSet(t)
    <-done
}

func benchmarkRandomGetWhenMerge(t *testing.T) {
    done := make(chan bool, 1)
    go func() {
        zForBenchmark.Merge()
        done <- true
    }()
    benchmarkRandomGet(t)
    <-done
}

func generateKeysForBenchmark(operationsNum, keySize int) {
    keysForBenchmark = make([]string, operationsNum)
    for i := 0; i < operationsNum; i++ {
        keysForBenchmark[i] = randomString(keySize)
    }
}

func generateValuesForBenchmark(operationsNum, valueSize int) {
    valuesForBenchmark = make([][]byte, operationsNum)
    for i := 0; i < operationsNum; i++ {
        valuesForBenchmark[i] = randomBytes(valueSize)
    }
}

func initZCaskForBenchmark() {
    // zcask configuration
    var err error
    opt := Option {
        dataFileDirectory: dataFileDirectoryForBenchmark,
        minDataFileId:      0,
        maxDataFileId:      12345678901234567890,
        maxKeySize:         10240,
        maxValueSize:       10240,
        maxDataFileSize:    1024*1024*32,
        writeBufferSize:    1024*1024*4,
        isLoadOldDataFile:  false,
        maxOpenOldDataFile: 128,
        maxCacheOldDataFile: 128,
    }

    zForBenchmark, err = NewZCask(opt)
    if err != nil {
        log.Fatalf("new zcask failed, details: %v", err)
    }
}

func TestBenchmarkAll(t *testing.T) {
    // init
    operationsNumForBenchmark = 1e5

    randomSetNumForBenchmark  = 1e5
    randomGetNumForBenchmark  = 1e5

    keySizeForBenchmark = 9
    valueSizeForBenchmark = 1024*2

    generateKeysForBenchmark(
        operationsNumForBenchmark,
        keySizeForBenchmark)

    generateValuesForBenchmark(
        operationsNumForBenchmark,
        valueSizeForBenchmark)

    dataFileDirectoryForBenchmark = "test_data_for_TestBenchmarkAll"

    if err := os.RemoveAll(dataFileDirectoryForBenchmark); err != nil {
        t.Log(err)
    }
    if err := os.Mkdir(dataFileDirectoryForBenchmark, 0700); err != nil {
        t.Log(err)
    }
    initZCaskForBenchmark()
    if err := zForBenchmark.Start(); err != nil {
        t.Fatal("zcask start failed.")
    }

    log.Println("RandomSet:")
    benchmarkRandomSet(t)
    log.Println("==================")

    log.Println("RandomGet:")
    benchmarkRandomGet(t)
    log.Println("==================")

    log.Println("RandomGetWhenMerge:")
    benchmarkRandomGetWhenMerge(t)
    log.Println("==================")

    log.Println("RandomSetWhenMerge:")
    benchmarkRandomSetWhenMerge(t)
    log.Println("==================")

    if err := zForBenchmark.ShutDown(); err != nil {
        t.Fatal("zcask shutdown failed, details: %p", err)
    }
}

func TestGetEveryKey(t *testing.T) {
    operatesNum := 5000
    testDataDirectory := "./test_data_for_TestGetEveryKey"

    if err := os.RemoveAll(testDataDirectory); err != nil {
        t.Fatal(err)
    }
    if err := os.Mkdir(testDataDirectory, 0700); err != nil {
        t.Fatal(err)
    }

    // zcask configuration
    opt := Option {
        dataFileDirectory: testDataDirectory,
        minDataFileId: 0,
        maxDataFileId: 12345678901234567890,
        maxKeySize: 1024,
        maxValueSize: 1024,
        maxDataFileSize: 1024*512,
        writeBufferSize: 1024,
        isLoadOldDataFile: false,
        maxOpenOldDataFile: 128,
        maxCacheOldDataFile: 4,
    }

    z, err := NewZCask(opt)
    err = z.Start()
    if err != nil {
        t.Fatalf("zcask start failed, details: %v", err)
    }

    keys := make([]string, 0, operatesNum)
    values := make([][]byte, 0, operatesNum)

    for i := 0; i < operatesNum; i++ {
        ksize := rand.Intn(32) + 10
        vsize := rand.Intn(64) + 10
        key := randomString(ksize)
        value := randomBytes(vsize)

        keys = append(keys, key)
        values = append(values, value)

        err := z.Set(key, value, 0)
        if err != nil {
           t.Fatalf("zcask set <%s, %s> failed, details: %v",
               key, string(value), err)
        }
        for j := len(keys)-1; j >= 0; j-- {
            v, err := z.Get(keys[j])
            if err != nil {
                t.Fatalf("get key<%s> failed, details: %v", keys[j], err)
            }
            assertEqualByteSlice(values[j], v, t)
        }
    }

    if err = z.ShutDown(); err != nil {
        t.Fatal("zcask shutdown failed, details: %p", err)
    }
}

func TestRandomSetAndGetAndDelete(t *testing.T) {
    operatesNum := 100000
    testDataDirectory := "./test_data_for_TestRandomSetAndGetAndDelete"

    if err := os.RemoveAll(testDataDirectory); err != nil {
        t.Log(err)
    }
    if err := os.Mkdir(testDataDirectory, 0700); err != nil {
        t.Log(err)
    }

    // zcask configuration
    opt := Option {
        dataFileDirectory: testDataDirectory,
        minDataFileId: 0,
        maxDataFileId: 12345678901234567890,
        maxKeySize: 1024,
        maxValueSize: 10240,
        maxDataFileSize: 32 << 20,
        writeBufferSize: 4 << 20,
        isLoadOldDataFile: false,
        maxOpenOldDataFile: 128,
        maxCacheOldDataFile: 4,
    }

    z, err := NewZCask(opt)
    err = z.Start()
    if err != nil {
        t.Fatalf("zcask start failed, details: %v", err)
    }

    dict := make(map[string][]byte)
    var keys []string
    var values [][]byte
    for i := 0; i < operatesNum; i++ {
        op := rand.Intn(3)
        if op == 0 {
            // Set
            ksize := rand.Intn(22) + 10
            vsize := rand.Intn(2048) + 10
            key := randomString(ksize)
            value := randomBytes(vsize)

            dict[key] = value

            err := z.Set(key, value, 0)
            if err != nil {
                t.Fatalf("zcask set <%s, %s> failed, details: %v",
                    key, string(value), err)
            }

            keys = append(keys, key)
            values = append(values, value)
        } else if op == 1 {
            // Get
            if len(keys) == 0 {
                continue
            }

            idx := rand.Intn(len(keys))
            key := keys[idx]

            dv, ok := dict[key]
            zv, err := z.Get(key)

            if !ok {
                if err == errorKeyNotExisted {
                    continue
                } else {
                    t.Fatalf("key %s was not existed, but get from zcask, details: %v", key, err)
                }
            } else {
                if err != nil {
                    t.Fatalf("seted key %s, but can't get from zcask, details: %v", key, err)
                } else {
                    assertEqualByteSlice(dv, zv, t)
                }
            }
        } else {
            // Delete
            if len(keys) == 0 {
                continue
            }

            idx := rand.Intn(len(keys))
            key := keys[idx]
            delete(dict, key)
            z.Delete(key)
        }
    }

    if err = z.ShutDown(); err != nil {
        t.Fatal("zcask shutdown failed, details: %p", err)
    }
}
