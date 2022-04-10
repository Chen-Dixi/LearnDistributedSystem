# LearnDistributedSystem

课程地址：[**https://pdos.csail.mit.edu/6.824/schedule.html**](https://pdos.csail.mit.edu/6.824/schedule.html)

## Lab1
### Run  

在`src/main`路径 运行:
```
go build -race -buildmode=plugin ../mrapps/wc.go
```
```
go run -race mrcoordinator.go pg-*.txt
```

之后多个worker可以并发执行，启动多个终端运行下面程序：
```
go run -race mrworker.go wc.so
```

`src/mr/`路径下为自己实现的代码

## Lab2
- [**Lab2A**](https://github.com/Chen-Dixi/MIT6.824/blob/2A/src/raft/raft.go)

```bash
go test -run 2A -race
```

- [**Lab2B**](https://github.com/Chen-Dixi/MIT6.824/blob/2B/src/raft/raft.go)

```bash
go test -run 2B -race
```