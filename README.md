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
- [ ] Buffer Pool Manager Instance
- [ ] Parallel Buffer Pool Manager

