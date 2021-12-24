# SimpleDB

## Introduce
A simple database from CMU 15-445 course.

## Build
```shell
docker build -t simpledb .  
docker run -itd --name simpledb --network=host -v {your project path}:/simpledb simpledb /bin/bash  
docker exec -it simpledb /bin/bash
```

## Schedule
### C++ Primer
- [x] C++ Primer

### Buffer Pool Manager
- [x] LRU Replacement Policy
- [x] Buffer Pool Manager Instance
- [x] Parallel Buffer Pool Manager

### Hash Index
- [x] Page Layouts
- [x] Hash Table Implementation
- [ ] Concurrency Control

### Query Execution
- [ ] Sequential Scan
- [ ] Insert
- [ ] Update
- [ ] Delete
- [ ] Nested Loop Join
- [ ] Hash Join
- [ ] Aggregation
- [ ] Limit
- [ ] Distinct

