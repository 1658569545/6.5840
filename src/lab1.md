## 介绍  
在本实验中，你将构建一个 MapReduce 系统。你需要实现一个工作进程（worker），该进程调用应用程序的 Map 和 Reduce 函数，并负责读取和写入文件。此外，你还需要实现一个协调进程（coordinator），它负责分配任务给工作进程，并处理失败的工作进程。你将构建的系统类似于 MapReduce 论文中描述的架构。（注意：本实验使用 "coordinator" 这个术语，而不是论文中的 "master"。）  

## 开始  
首先，你需要安装 Go 以完成实验。  

使用 Git（版本控制系统）获取实验的初始代码。如果你想了解更多关于 Git 的信息，可以参考《Pro Git》或 Git 用户手册。  

```sh
$ git clone git://g.csail.mit.edu/6.5840-golabs-2024 6.5840
$ cd 6.5840
$ ls
Makefile  src
```
  
我们提供了一个简单的顺序 MapReduce 实现 `src/main/mrsequential.go`。它在单个进程中依次运行 Map 和 Reduce 任务。此外，我们还提供了两个 MapReduce 应用程序：  
- 词频统计（word count）：`mrapps/wc.go`
- 文本索引器（text indexer）：`mrapps/indexer.go`  

你可以按以下步骤顺序运行词频统计：  

```sh
$ cd ~/6.5840
$ cd src/main
$ go build -buildmode=plugin ../mrapps/wc.go
$ rm mr-out*
$ go run mrsequential.go wc.so pg*.txt
$ more mr-out-0
```
示例输出：
```
A 509
ABOUT 2
ACT 8
...
```
`mrsequential.go` 会将结果输出到 `mr-out-0` 文件，输入是 `pg-xxx.txt` 这些文本文件。  

你可以参考 `mrsequential.go` 代码，并查看 `mrapps/wc.go` 以了解 MapReduce 应用程序的代码结构。  

在本实验及后续实验中，我们可能会更新提供的代码。为了能够使用 `git pull` 轻松获取这些更新并合并，请尽量保持我们提供的代码文件不变。你可以按照实验的要求在这些文件中添加代码，但不要随意移动它们。你也可以在新文件中编写自己的函数。  

## 你的任务（中等/困难）  
你需要实现一个分布式 MapReduce 系统，包括以下两个程序：  
1. **协调进程（coordinator）**：只有一个协调进程。  
2. **工作进程（worker）**：可以有一个或多个，并行执行。  

在真实系统中，多个工作进程通常运行在不同的机器上，但在本实验中，所有进程都将在同一台机器上运行。工作进程将通过 RPC 与协调进程通信。  

工作进程的执行逻辑如下：  
1. 在循环中向协调进程请求任务。  
2. 读取任务的输入文件。  
3. 执行任务并写入输出文件。  
4. 再次向协调进程请求新任务。  

协调进程需要能够检测到某个工作进程在合理的时间内（本实验设定为 10 秒）未完成任务，并将该任务分配给其他工作进程。  

我们提供了一些代码作为起点：  
- **coordinator 主程序**：`main/mrcoordinator.go`  
- **worker 主程序**：`main/mrworker.go`  
> **注意**：不要修改 `mrcoordinator.go` 和 `mrworker.go` 文件。  

你的实现应该放在：
- `mr/coordinator.go`
- `mr/worker.go`
- `mr/rpc.go`

### 运行你的代码（以词频统计为例）  
首先，确保 `wc.go` 插件已编译：
```sh
$ go build -buildmode=plugin ../mrapps/wc.go
```
在 `src/main` 目录下，运行协调进程：
```sh
$ rm mr-out*
$ go run mrcoordinator.go pg-*.txt
```
这里的 `pg-*.txt` 作为输入文件，每个文件对应一个 Map 任务。  

在一个或多个终端窗口中，运行工作进程：
```sh
$ go run mrworker.go wc.so
```
当所有任务完成后，查看输出：
```sh
$ cat mr-out-* | sort | more
```
示例输出：
```
A 509
ABOUT 2
ACT 8
...
```

## 测试脚本  
我们提供了 `main/test-mr.sh` 测试脚本，检查 `wc` 和 `indexer` 任务的正确性，并测试 Map 和 Reduce 任务的并行性，以及在某些工作进程崩溃的情况下的恢复能力。  

如果你现在运行测试脚本，它会卡住，因为协调进程不会自动退出：
```sh
$ cd ~/6.5840/src/main
$ bash test-mr.sh
*** Starting wc test.
```
你可以在 `mr/coordinator.go` 中将 `Done` 函数的 `ret := false` 改为 `true`，使协调进程立即退出。然后重新运行测试：
```sh
$ bash test-mr.sh
*** Starting wc test.
sort: No such file or directory
cmp: EOF on mr-wc-all
--- wc output is not the same as mr-correct-wc.txt
--- wc test: FAIL
```
此时测试失败，因为 `mr/coordinator.go` 和 `mr/worker.go` 代码尚未完成，没有正确生成 `mr-out-*` 文件。  

当你完成实验后，测试脚本的输出应如下：
```sh
$ bash test-mr.sh
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting job count test.
--- job count test: PASS
*** Starting early exit test.
--- early exit test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS
```
你可能会在 Go RPC 库中看到类似的错误信息：
```
2019/12/16 13:27:09 rpc.Register: method "Done" has 1 input parameters; needs exactly three
```
可以忽略这些错误。因为 Go RPC 服务器在注册 `coordinator` 方法时，会检查其参数数量是否符合 RPC 规则，而 `Done` 方法并未用于 RPC。  

此外，根据你的 Worker 进程终止策略，你可能还会看到类似的错误：
```
2024/02/11 16:21:32 dialing: dial unix /var/tmp/5840-mr-501: connect: connection refused
```
如果每次测试中只出现少量这样的错误信息，是可以接受的。这些错误表示 Worker 进程在协调进程已经退出后，仍尝试进行 RPC 通信导致失败。


## 一些规则：

1. **Map阶段**应将中间键分为`nReduce`个桶，以供`nReduce`个Reduce任务使用，其中`nReduce`是`main/mrcoordinator.go`传递给`MakeCoordinator()`的参数。每个Mapper应为Reduce任务创建`nReduce`个中间文件。

2. **Worker实现**应将第`X`个Reduce任务的输出放入文件`mr-out-X`中。

3. 每个`mr-out-X`文件应包含每个Reduce函数输出的一行。每行应该使用Go中的`"%v %v"`格式生成，格式化键和值。查看`main/mrsequential.go`中的注释“this is the correct format”，如果你的实现格式偏差太大，测试脚本会失败。

4. 你可以修改`mr/worker.go`、`mr/coordinator.go`和`mr/rpc.go`。可以暂时修改其他文件进行测试，但确保你的代码能与原始版本一起工作，我们将使用原始版本进行测试。

5. Worker应将中间的Map输出存储在当前目录中的文件中，供Worker稍后作为Reduce任务的输入读取。

6. `main/mrcoordinator.go`期望`mr/coordinator.go`实现一个`Done()`方法，当MapReduce作业完全完成时返回`true`，此时`mrcoordinator.go`将退出。

7. 当作业完全完成时，Worker进程应退出。一种简单的实现方式是使用`call()`返回值：如果Worker无法联系到Coordinator，它可以假设Coordinator已经退出，作业完成，因此Worker也可以终止。根据你的设计，你可能还会发现使用一个“请退出”的伪任务会很有帮助，Coordinator可以将这个任务交给Workers。

## 提示：

1. **开发与调试建议**：请查看“Guidance”页面，其中有一些开发和调试的技巧。

2. 开始的一种方式是修改`mr/worker.go`中的`Worker()`，向Coordinator发送RPC请求任务。然后修改Coordinator，以响应一个尚未开始的Map任务的文件名。然后修改Worker以读取该文件并调用应用程序的Map函数，就像在`mrsequential.go`中一样。

3. 应用程序的Map和Reduce函数在运行时使用Go的插件包加载，文件名以`.so`结尾。

4. 如果你更改了`mr/`目录中的任何内容，你可能需要重新构建使用的MapReduce插件，例如运行`go build -buildmode=plugin ../mrapps/wc.go`。

5. 本实验依赖于Workers共享一个文件系统。在所有Worker都运行在同一台机器上时，这很简单，但如果Worker运行在不同的机器上，则需要像GFS这样的全局文件系统。

6. 一个合理的中间文件命名规范是`mr-X-Y`，其中`X`是Map任务编号，`Y`是Reduce任务编号。

7. Worker的Map任务代码将需要一种存储中间键/值对的方式，以便在Reduce任务期间能够正确读取这些文件。一种可能性是使用Go的`encoding/json`包。可以通过如下方式将键值对以JSON格式写入打开的文件：
   ```go
   enc := json.NewEncoder(file)
   for _, kv := ... {
       err := enc.Encode(&kv)
   }
   ```
   读取该文件的方式：
   ```go
   dec := json.NewDecoder(file)
   for {
       var kv KeyValue
       if err := dec.Decode(&kv); err != nil {
           break
       }
       kva = append(kva, kv)
   }
   ```

8. Worker的Map任务代码可以使用`ihash(key)`函数（在`worker.go`中）来选择给定键的Reduce任务。

9. 你可以从`mrsequential.go`中偷一些代码，用于读取Map输入文件，排序Map和Reduce之间的中间键/值对，以及将Reduce输出存储在文件中。

10. Coordinator作为RPC服务器会并发运行，别忘了锁定共享数据。

11. 使用Go的竞态检测器，使用命令`go run -race`。`test-mr.sh`文件的开头有一条注释，告诉你如何使用`-race`运行它。当我们评分时，不会使用竞态检测器。然而，如果你的代码有竞态条件，很可能在没有竞态检测器的情况下也会失败。

12. Workers有时需要等待，例如，Reduce任务不能在最后一个Map任务完成之前开始。一种可能性是Workers定期向Coordinator请求任务，在每个请求之间使用`time.Sleep()`休眠。另一种可能性是，在Coordinator中相关的RPC处理程序有一个循环，等待任务，可以使用`time.Sleep()`或`sync.Cond`。Go为每个RPC运行一个单独的线程，因此一个处理程序的等待不会阻止Coordinator处理其他RPC。

13. Coordinator不能可靠地区分崩溃的Workers、活着但卡住的Workers和正在执行但太慢的Workers。最好的办法是让Coordinator等待一段时间，然后放弃并重新给不同的Worker分配任务。本实验要求Coordinator等待10秒钟；之后，Coordinator应该假设Worker已经死掉（当然，它可能并没有）。

14. 如果你选择实现备份任务（第3.6节），请注意我们会测试你的代码是否在Workers执行任务时没有崩溃时调度了多余的任务。备份任务应该只在经过一段较长的时间（例如10秒）后调度。

15. 要测试崩溃恢复，可以使用`mrapps/crash.go`应用程序插件。它会在Map和Reduce函数中随机退出。

16. 为了确保在崩溃发生时不会有人看到部分写入的文件，MapReduce论文提到了使用临时文件的技巧，并在完全写入后原子地重命名文件。你可以使用`ioutil.TempFile`（或者Go 1.17及以后的版本使用`os.CreateTemp`）来创建临时文件，并使用`os.Rename`来原子地重命名它。

17. `test-mr.sh`将在子目录`mr-tmp`中运行所有进程，因此如果发生错误且你想查看中间或输出文件，可以查看那里。你可以暂时修改`test-mr.sh`，使其在失败的测试后退出，这样脚本就不会继续测试（并覆盖输出文件）。

18. `test-mr-many.sh`会多次运行`test-mr.sh`，你可能需要这样做，以便发现低概率的错误。它接受一个参数，表示测试运行的次数。你不应该并行运行多个`test-mr.sh`实例，因为Coordinator会重用相同的套接字，从而导致冲突。

19. Go的RPC仅发送字段名以大写字母开头的结构体字段。子结构体也必须具有大写字段名。

20. 调用RPC时，`call()`函数的回复结构应包含所有默认值。RPC调用应该像这样：
   ```go
   reply := SomeType{}
   call(..., &reply)
   ```
   不要在调用前设置`reply`结构体的任何字段。如果你传递了具有非默认字段的`reply`结构体，RPC系统可能会悄悄地返回错误的值。

## 实验思路：