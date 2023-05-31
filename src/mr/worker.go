package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KeyValueSort []KeyValue

func (k KeyValueSort) Len() int           { return len(k) }
func (k KeyValueSort) Swap(i, j int)      { k[i], k[j] = k[i], k[j] }
func (k KeyValueSort) Less(i, j int) bool { return k[i].Key < k[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		task := GetTask() //获取任务类型
		switch task.Tasktype {
		case MapTasktype:
			{ //执行map任务
				DoMapTask(mapf, &task)
				callDone(&task)
			}
		case ReduceTasktype:
			{ //执行reduce任务
				DoReduceTask(reducef, &task)
				callDone(&task)
			}
		case WaittingTask:
			{ //等待分配任务
				time.Sleep(time.Second * 5)
				callDone(&task)
			}
		case ExitTask:
			{ //任务结束退出
				fmt.Println("Task about :[", task.Taskid, "] is finished")
				break
			}
		}
	}
}

func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	var intermediate []KeyValue //存储中间文件
	filename := response.Filename[0]

	file, err := os.Open(filename) //打开文件
	if err != nil {
		fmt.Printf("%v open failed", filename)
	}
	content, err := ioutil.ReadAll(file) //读取文件内容
	if err != nil {
		fmt.Println("Read content failed....")
	}
	file.Close()
	intermediate = mapf(filename, string(content))

	rn := response.ReduceNum
	HashKV := make([][]KeyValue, rn) //对每一个键值对kv，根据key哈希后的值对rn取余，将key相同的kv分配到对应的ReduceNUm
	for _, kv := range intermediate {
		HashKV[ihash(kv.Key)%rn] = append(HashKV[ihash(kv.Key)%rn], kv)
	}
	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(response.Taskid) + "-" + strconv.Itoa(i) //设置reduceid对应的中间文件的名称
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashKV[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}
}
func DoReduceTask(reducef func(string, []string) string, response *Task) {
	reduceFilenum := response.Taskid
	intermediate := shuffle(response.Filename) // response.Filename = mr-tmp-*-reduceFilenum
	dir, _ := os.Getwd()
	tempfile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to creat temp file", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) {
			if intermediate[i].Key == intermediate[j].Key {
				j++
			}
		}
		var Value []string
		for k := i; k < j; k++ {
			Value = append(Value, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, Value)
		fmt.Fprintf(tempfile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempfile.Close()
	fn := fmt.Sprintf("mr-out-%d", reduceFilenum)
	os.Rename(tempfile.Name(), fn)

}

//对于Hash后具有相同的reduce_id的文件进行合并，文件内的keyvalue由小到大进行排序,(注意，同一个文件内的key值不一定相同)
func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(KeyValueSort(kva))
	return kva
}
func callDone(f *Task) Task {
	args := f
	reply := Task{}
	call("Coordinator.Markfinshed", &args, &reply)
	return reply
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func GetTask() Task {
	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.Polltask", &args, &reply)
	if ok {
		fmt.Println(reply)
	} else {
		fmt.Println("call failed")
	}
	return reply
}
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
