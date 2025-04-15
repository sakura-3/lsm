参考[leveldb](https://github.com/google/leveldb),基于LSM架构的KV存储系统

## TODO

- [x] 基础模块
  - [x] 基于skiplist的memtable
  - [x] sorted string table(sstable)
  - [x] wal日志
  - [x] compaction过程
  - [x] db实例
- [ ] 集成测试
- [ ] 可变长编码
- [ ] 从wal日志恢复
- [ ] snapshot功能
