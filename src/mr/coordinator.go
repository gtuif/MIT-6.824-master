package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var mu sync.Mutex

type Coordinator struct {
	// Your definitions here.
	ReduceNum         int //需要的reducer 的个数
	Taskid            int
	DistPhase         Phase //整个任务所处的阶段
	TaskChannelReduce chan *Task
	TaskChannelMap    chan *Task
	taskMetaHolder    TaskMetaHolder //
	files             []string       //传入的文件
}

//保存任务的元数据
type TaskMetaInfo struct {
	TaskAddr  *Task     //任务的指针
	state     TaskState //任务状态,当任务被放进通道中，我们能通过地址将这个任务标记为完成
	StartTime time.Time //任务开始的时间
}
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Polltask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch c.DistPhase {
	case MapPhase:
		{
			if len(c.TaskChannelMap) > 0 {
				*reply = *<-c.TaskChannelMap
				if !c.taskMetaHolder.judgestate(reply.Taskid) {
					fmt.Println("task[%d] is running\n", reply.Taskid)
				}
			} else {
				reply.Tasktype = WaittingTask //map任务已经被分配完了，所以要把状态改为等待新的map任务
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.TaskChannelReduce) > 0 {
				*reply = *<-c.TaskChannelReduce
				if !c.taskMetaHolder.judgestate(reply.Taskid) {
					fmt.Println("task[%d] is running\n", reply.Taskid)
				}
			} else {
				reply.Tasktype = WaittingTask
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case AllDone:
		{
			reply.Tasktype = ExitTask
			fmt.Println("The Task is ALLDone\n")
		}
	default:
		{
			panic("The Phase is undefined")
		}
	}
	return nil
}
func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {
		c.makeReduceTask()
		c.DistPhase = ReducePhase
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}
func (t *TaskMetaHolder) judgestate(Taskid int) bool {
	taskinfo, ok := t.MetaMap[Taskid]
	if !ok || taskinfo.state != Waiting {
		return false
	}
	taskinfo.state = Working
	taskinfo.StartTime = time.Now()
	return true
}
func (t *TaskMetaHolder) checkTaskDone() bool {
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)
	for _, v := range t.MetaMap {
		if v.TaskAddr.Tasktype == MapTasktype {
			if v.state == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskAddr.Tasktype == ReduceTasktype {
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else {
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}
	return false
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	// Your code here.
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone {
		fmt.Println("AllTask is finished, The coordinator will exit")
		return true
	} else {
		return false
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReduceNum:         nReduce,
		DistPhase:         MapPhase,
		TaskChannelMap:    make(chan *Task, len(files)),
		TaskChannelReduce: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}

	// Your code here.
	c.makeMapTask(files)
	c.makeReduceTask()
	c.server()
	return &c
}

//生成一个map任务,并放进通道中
func (c *Coordinator) makeMapTask(files []string) {
	for _, v := range files {
		id := c.generateid()
		task := Task{
			Tasktype:  MapTasktype,
			Taskid:    id,
			ReduceNum: c.ReduceNum,
			Filename:  []string{v},
		}
		taskMetaInfo := TaskMetaInfo{
			TaskAddr: &task,
			state:    Waiting,
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		fmt.Println("make a map task:", &task)
		c.TaskChannelMap <- &task
	}
}
func (c *Coordinator) makeReduceTask() {
	for i := 0; i < c.ReduceNum; i++ {
		id := c.generateid()
		task := Task{
			Tasktype:  ReduceTasktype,
			Taskid:    id,
			ReduceNum: c.ReduceNum,
			Filename:  selectReduceNum(i),
		}
		taskMetaInfo := TaskMetaInfo{
			TaskAddr: &task,
			state:    Waiting,
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)
		fmt.Println("A Reduce Task is maked")
		c.TaskChannelReduce <- &task
	}
}
func selectReduceNum(ReduceNum int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, r := range files {
		if strings.HasPrefix(r.Name(), "mr-tmp") && strings.HasSuffix(r.Name(), strconv.Itoa(ReduceNum)) {
			s = append(s, r.Name())
		}
	}
	return s
}
func (c *Coordinator) generateid() int {
	res := c.Taskid
	c.Taskid++
	return res
}
func (t *TaskMetaHolder) acceptMeta(taskMetaInfo *TaskMetaInfo) bool {
	Taskid := taskMetaInfo.TaskAddr.Taskid
	meta, _ := t.MetaMap[Taskid]
	if meta != nil {
		return false
	} else {
		t.MetaMap[Taskid] = taskMetaInfo
	}
	return true
}
func (c *Coordinator) Markfinshed(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.Tasktype {
	case MapTasktype:
		{
			meta, ok := c.taskMetaHolder.MetaMap[args.Taskid]
			if ok && meta.state == Working {
				meta.state = Done
				fmt.Println("MapTaskId[%d] is finished", args.Taskid)
			} else {
				fmt.Println("MapTaskId[%d] is already finshed", args.Taskid)
			}
			break
		}
	default:
		panic("The Tasktype is not defined")
	}
	return nil
}
