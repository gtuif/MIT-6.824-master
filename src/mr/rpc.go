package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Task struct {
	Tasktype  Tasktype // 判断是否task是map还是reduce
	Taskid    int      //任务的id
	ReduceNum int      //传入的reduce的个数
	Filename  []string //输入文件
}
type TaskArgs struct{} //传给rpc的参数

type Tasktype int //任务类型的父类型

type Phase int //阶段的父类型

type TaskState int //任务状态的类型

const (
	MapTasktype Tasktype = iota
	ReduceTasktype
	WaittingTask //表示任务都已经分发完成，但是任务并没有完成
	ExitTask     //任务完成退出
)
const (
	MapPhase    Phase = iota //此时在分发Map任务
	ReducePhase              //此时在分发Redeuce任务
	AllDone                  //任务分配结束
)
const (
	Working TaskState = iota //正在执行
	Waiting                  //等待执行
	Done                     //执行完成
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
