# ZKawa
a Go implementation of key-value storage system based on bitcask model.

## Go version
go1.7.3 linux/amd64

## Why zkawa
This storage is dedicated to a very special friend **Z**hehao. **Kawa**, the Japanese word for cuteness.

## Installation
### install zcask
* go get github.com/scaugrated/zkawa/zcask

## ZCask Benchmark
**RandomSet:**

```
100000 set operation[key: 9B, value: 2048B] in 0.320759s
average 311760.379675 qps
average 635.991175 MB/s
average 3.207592 micros/op
```

**RandomGet:**

```
100000 get operation in 0.663533s
average 150708.328087 qps
average 6.635333 micros/op
```


**RandomGetWhenMerge:**

```
100000 get operation in 1.108773s
average 90189.750008 qps
average 11.087734 micros/op
```


**RandomSetWhenMerge:**

```
100000 set operation[key: 9B, value: 2048B] in 0.986591s
average 101359.118131 qps
average 206.772601 MB/s
average 9.865911 micros/op
```
