// Copyright (c) 2017, The zkawa Authors.
// All rights reserved.
//
// Author: Zheng Gonglin <scaugrated@gmail.com>
// Created: 2017/05/31

package zcask

import (
    "log"
    "math/rand"
    "os"
    "testing"
    "time"
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

    dataFileDirectoryForBenchmark = "test_data_for_benchmark"

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
        v, err := zForBenchmark.Get(keysForBenchmark[r])
        if err != nil {
            t.Fatalf("Get Record[key:%s] failed", keysForBenchmark[r])
        }
        assertEqualByteSlice(v, valuesForBenchmark[r], t)
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
        DataFileDirectory: dataFileDirectoryForBenchmark,
        MinDataFileId: 0,
        MaxDataFileId: 12345678901234567890,
        MaxKeySize: 10240,
        MaxValueSize: 10240,
        MaxDataFileSize: 32<<20, //1024 * 1024 * 512,
        IsLoadOldDataFile: false,
    }

    zForBenchmark, err = NewZCask(opt)
    if err != nil {
        log.Fatalf("new zcask failed, details: %v", err)
    }
}

func TestBenchmarkAll(t *testing.T) {
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
    defer zForBenchmark.ShutDown()

    log.Println("RandomSet:")
    benchmarkRandomSet(t)
    log.Println("==================")

    log.Println("RandomGet:")
    benchmarkRandomGet(t)
    log.Println("==================")

    log.Println("RandomSetWhenMerge:")
    benchmarkRandomSetWhenMerge(t)
    log.Println("==================")

    log.Println("RandomGetWhenMerge:")
    benchmarkRandomGetWhenMerge(t)
}
