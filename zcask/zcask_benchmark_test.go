// Copyright (c) 2017, The zkawa Authors.
// All rights reserved.
//
// Author: Zheng Gonglin <scaugrated@gmail.com>
// Created: 2017/05/31

package zcask

import (
    "log"
    "math/rand"
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

    operationsNumForBenchmark = 1e2

    randomSetNumForBenchmark  = 1e5
    randomGetNumForBenchmark  = 1e5

    keySizeForBenchmark = 9
    valueSizeForBenchmark = 2048

    generateKeysForBenchmark(
        operationsNumForBenchmark,
        keySizeForBenchmark)

    generateValuesForBenchmark(
        operationsNumForBenchmark,
        valueSizeForBenchmark)

    dataFileDirectoryForBenchmark = "test_data_for_benchmark"

}

func benchmarkRandomSet(b *testing.B) {
	b.Log("RandomSet:")
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
	b.Logf("%d set operation[key: %dB, value: %dB] in %fs\n", operationsNumForBenchmark, keySizeForBenchmark, valueSizeForBenchmark, d.Seconds())
	b.Logf("average %f qps\n", float64(operationsNumForBenchmark)/d.Seconds())
	writeMB := int64(operationsNumForBenchmark) * int64(valueSizeForBenchmark) / 1e6
	b.Logf("average %f MB/s\n", float64(writeMB)/d.Seconds())
	b.Logf("average %f micros/op\n", d.Seconds()*1e6/float64(operationsNumForBenchmark))
}

func benchmarkRandomGet(b *testing.B) {
	b.Log("RandomGet:")
	begin := time.Now()
	for i := 0; i < operationsNumForBenchmark; i++ {
		r := rand.Intn(len(keysForBenchmark))
		if _, err := zForBenchmark.Get(keysForBenchmark[r]); err != nil {
			b.Fatalf("Get Record[key:%s] failed", keysForBenchmark[r])
		}
	}
	end := time.Now()
	d := end.Sub(begin)
	b.Logf("%d get operation in %fs\n", operationsNumForBenchmark, d.Seconds())
	b.Logf("average %f qps\n", float64(operationsNumForBenchmark)/d.Seconds())
	b.Logf("average %f micros/op\n", d.Seconds()*1e6/float64(operationsNumForBenchmark))
}

func benchmarkRandomSetWhenMerge(b *testing.B) {
	b.Log("RandomSetWhenMerge:")
	done := make(chan bool, 1)
	go func() {
		zForBenchmark.Merge()
		done <- true
	}()
	benchmarkRandomSet(b)
	<-done
}

func benchmarkRandomGetWhenMerge(b *testing.B) {
	b.Log("RandomGetWhenMerge:")
	done := make(chan bool, 1)
	go func() {
		zForBenchmark.Merge()
		done <- true
	}()
	benchmarkRandomGet(b)
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
        MaxDataFileSize: 1024 * 1024 * 32,
        IsLoadOldDataFile: false,
    }

    zForBenchmark, err = NewZCask(opt)
    if err != nil {
        log.Fatalf("new zcask failed, details: %v", err)
    }
}

func BenchmarkAll(b *testing.B) {
    initZCaskForBenchmark()
    if err := zForBenchmark.Start(); err != nil {
        b.Fatal("zcask start failed.")
    }
    defer zForBenchmark.ShutDown()

    benchmarkRandomSet(b)
    b.Log("==================")
    benchmarkRandomGet(b)
    b.Log("==================")
    benchmarkRandomSetWhenMerge(b)
    b.Log("==================")
    benchmarkRandomGetWhenMerge(b)
}
