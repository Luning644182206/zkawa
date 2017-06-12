// Copyright (c) 2017, The zkawa Authors.
// All rights reserved.
//
// Author: Zheng Gonglin <scaugrated@gmail.com>
// Created: 2017/02/18

package zcask

import (
    "fmt"
)

type TableValue struct {
    dataFileId  uint64
    timestamp   uint64
    expiration  uint64
    zRecordPos  int64
    // deleted flag only use for zcask.Load.
    isDeleted   bool
}

type HashTable struct {
    table map[string] *TableValue
}

func NewHashTable() (*HashTable, error) {
    return &HashTable{
        table: make(map[string]*TableValue, 1024),
    }, nil
}

func (ht *HashTable) Get(key string, timestamp uint64) (*TableValue, error) {
    tv, ok := ht.table[key]
    if ok == false {
        return nil, errorKeyNotExisted
    }

    // IsDeleted only use in zcask.Load
    // don't check it in hash_table.Get

    if tv.expiration > 0 && tv.expiration <= timestamp {
        return nil, errorKeyExpired
    }

    return tv, nil
}

func (ht *HashTable) GetIncludeExpired(key string) (*TableValue, error) {
    tv, ok := ht.table[key]
    if ok == false {
        return nil, errorKeyNotExisted
    }

    // IsDeleted only use in zcask.Load
    // don't check it in hash_table.Get

    return tv, nil
}

func (ht *HashTable) Set(key string, value *TableValue) {
    ht.table[key] = value
}

func (ht *HashTable) Delete(key string) error {
    _, ok := ht.table[key]
    if ok == false {
        err := fmt.Errorf("delete key `%s` error, not exists.", key)
        return err
    }

    delete(ht.table, key)

    return nil
}

func (ht *HashTable) RemoveInvalidKeys() error {
    keys := make([]string, 0, 1024)
    for k, v := range ht.table {
        if v.isDeleted {
            keys = append(keys, k)
        }
    }
    for _, k := range keys {
        delete(ht.table, k)
    }
    return nil
}

func (ht *HashTable) Keys() []string {
    keys := make([]string, 0, len(ht.table))
    for k, _ := range ht.table {
        // include the key which IsDeleted is true.
        keys = append(keys, k)
    }
    return keys
}

func (ht *HashTable) Release() error {
    ht.table = nil
    return nil
}
