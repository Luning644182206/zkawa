// Copyright (c) 2017, The zkawa Authors.
// All rights reserved.
//
// Author: Zheng Gonglin <scaugrated@gmail.com>
// Created: 2017/05/26

package zcask

import (
    "math/rand"
    "os"
    "sort"
    "testing"
)

const (
    RandomCharSetForZCaskTesting = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    LengthOfRandomCharSetForZCaskTesting = len(RandomCharSetForZCaskTesting)
)

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
    setTimes = 50000
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
        randomKeys = append(randomKeys, randomString(32))
        randomValues = append(randomValues, randomBytes(64))
    }

    // zcask configuration
    opt := Option {
        DataFileDirectory: testDataDirectory,
        MinDataFileId: 0,
        MaxDataFileId: 12345678901234567890,
        MaxKeySize: 1024,
        MaxValueSize: 1024,
        MaxDataFileSize: 1024 * 1024 * 32,
        IsLoadOldDataFile: false,
    }

    z, err := NewZCask(opt)
    err = z.Start()
    if err != nil {
        t.Fatalf("zcask start failed, details: %v", err)
    }

    dict := make(map[string][]byte, setTimes)

    // testing zcask.Set
    t.Logf("setTimes: %d\n", setTimes)
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

    if err = z.ShutDown(); err != nil {
        t.Fatal("zcask shutdown failed, details: %p", err)
    }

    // testing Load
    opt.IsLoadOldDataFile = true
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
}
