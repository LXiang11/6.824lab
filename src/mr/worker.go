package mr

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/big"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type worker_run struct {
	//初始 0   map 1   reduce 2   exit 3
	state               int
	worker_id           int64
	current_map_task    map_task
	current_reduce_task reduce_task
	this_mapf           func(string, string) []KeyValue
	this_reducef        func(string, []string) string
}

type map_task struct {
	map_task_id int
	//中间文件名
	in_names []string
	nreduce  int
}

type reduce_task struct {
	reduce_id int
	//输出文件名
	output_name string
	nreduce     int
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	wr := &worker_run{0, nrand(), map_task{}, reduce_task{}, mapf, reducef}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// fmt.Println("worker have started!")
	wr.exec_run()

}

func (wr *worker_run) exec_run() {
	for wr.state != 3 {
		wr.CallExample()
		time.Sleep(time.Second)
	}
}

// 删除已存在的文件
func (wr *worker_run) clearFile(intermediate_name string) {

	_, error := os.Stat(intermediate_name)

	// check if error is "file not exists"
	if os.IsNotExist(error) {
		// fmt.Printf("%v file does not exist\n", intermediate_name)
	} else {
		//删除已存在的文件
		err := os.Remove(intermediate_name)
		if err != nil {
			log.Fatalf("cannot remove %v", intermediate_name)
		}
	}
}

func (wr *worker_run) exec_Map(files []string) {
	/*
		//json序列化
		enc := json.NewEncoder(file)
		for _, kv := ... {
		err := enc.Encode(&kv)
	*/
	// fmt.Println(wr.worker_id, "start map task", wr.current_map_task.map_task_id)
	intermediate := []KeyValue{}
	for _, filename := range files {
		// fmt.Println("map_file:", filename)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		//Map阶段，调用Map函数
		kva := wr.this_mapf(filename, string(content))
		//将kva打散逐个加入intermediate
		intermediate = append(intermediate, kva...)
	}

	intermediate_names := []string{}
	for i := 0; i < wr.current_map_task.nreduce; i++ {
		intermediate_name := "mr-" + strconv.Itoa(wr.current_map_task.map_task_id) + "-" + strconv.Itoa(i)
		intermediate_names = append(intermediate_names, intermediate_name)
		wr.clearFile(intermediate_name)
		intermediate_file, err := os.OpenFile(intermediate_name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0)
		if err != nil {
			log.Fatalf("cannot open %v", intermediate_name)
			return
		}
		defer intermediate_file.Close()
		// writer := bufio.NewWriter(intermediate_file)
		enc := json.NewEncoder(intermediate_file)
		for _, kv := range intermediate {
			if ihash(kv.Key)%wr.current_map_task.nreduce != i {
				continue
			}
			// _, err = writer.WriteString(kv.Key + " " + kv.Value)
			//json序列化
			err := enc.Encode(&kv)
			// _, err = intermediate_file.WriteString(kv.Key + " " + kv.Value + "\n")
			// fmt.Println(mt.in_name, "写入了：", kv.Key+" "+kv.Value+"\n")
			if err != nil {
				log.Fatalf("cannot write %v", intermediate_name)
				return
			}
		}
	}
	wr.current_map_task.in_names = intermediate_names

	// fmt.Println("中间文件：", intermediate_name)
	// writer.Flush()
}

func (wr *worker_run) exec_reduce(files []string) {
	// fmt.Println(wr.worker_id, "start reduce task", wr.current_reduce_task.reduce_id)
	/*
		//json反序列化
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	*/

	pattern := "mr-\\d+-" + strconv.Itoa(wr.current_reduce_task.reduce_id)
	matcher := regexp.MustCompile(pattern)

	pwd, pwderr := os.Getwd()

	if pwderr != nil {
		fmt.Println("Error Getwd:", pwderr)
	}

	err := filepath.Walk(pwd, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && matcher.MatchString(info.Name()) {
			files = append(files, info.Name())
		}
		return nil
	})

	if err != nil {
		fmt.Println("Error walking the path:", err)
	}
	var results []KeyValue
	for _, filename := range files {
		// fmt.Println("reduce_file", filename)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		// reader := bufio.NewReader(file)

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			results = append(results, kv)
		}

		defer file.Close()

		// // 按行处理txt
		// for {
		// 	line, _, err := reader.ReadLine()
		// 	if err == io.EOF {
		// 		break
		// 	}
		// 	kv := strings.Split(string(line), " ")
		// 	if ihash(kv[0])%rt.nreduce == rt.reduce_id {
		// 		results = append(results, KeyValue{kv[0], kv[1]})
		// 		// fmt.Println("属于", rt.reduce_id, "reduce任务的kv：", KeyValue{kv[0], kv[1]})
		// 	}
		// }
	}

	sort.Sort(ByKey(results))

	oname := "mr-out-" + strconv.Itoa(wr.current_reduce_task.reduce_id+1)
	// fmt.Println("输出文件：", oname)

	wr.clearFile(oname)

	wr.current_reduce_task.output_name = oname
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.

	i := 0
	for i < len(results) {
		j := i + 1
		for j < len(results) && results[j].Key == results[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, results[k].Value)
		}
		output := wr.this_reducef(results[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", results[i].Key, output)

		i = j
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func (wr *worker_run) CallExample() {

	// declare an argument structure.
	args := ExampleArgs{1, wr.current_map_task.map_task_id, wr.current_reduce_task.reduce_id, wr.state,
		wr.worker_id, wr.current_map_task.in_names, wr.current_reduce_task.output_name}

	// fill in the argument(s).
	// args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}
	// fmt.Println("&reply：", &reply)

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	// fmt.Println("&reply：", &reply)
	if ok {
		// fmt.Println("当前worker状态：", wr.state)
		if reply.Next_state == 1 {
			//map
			wr.current_map_task.map_task_id = reply.Map_id
			wr.current_map_task.nreduce = reply.Nreduce
			// if wr.state == 0 {
			// 	intermediate_name := "mr-" + strconv.Itoa(wr.current_map_task.map_task_id) + "-" + strconv.Itoa(wr.current_reduce_task.reduce_id) + ".txt"

			// 	_, error := os.Stat(intermediate_name)

			// 	// check if error is "file not exists"
			// 	if os.IsNotExist(error) {
			// 		// fmt.Printf("%v file does not exist\n", intermediate_name)
			// 	} else {
			// 		//删除已存在的文件
			// 		err := os.Remove(intermediate_name)
			// 		if err != nil {
			// 			log.Fatalf("cannot remove %v", intermediate_name)
			// 		}
			// 	}
			// }
			// fmt.Println("exec_map:", mt.map_id)
			wr.exec_Map(reply.Files_list)
		} else if reply.Next_state == 2 {
			//reduce
			wr.current_reduce_task.reduce_id = reply.Reduce_id
			wr.current_reduce_task.nreduce = reply.Nreduce
			// fmt.Println("exec_reduce:", rt.reduce_id)
			wr.exec_reduce(reply.Files_list)
		} else {
			//cycle_run
			// exec_run()
		}
		wr.state = reply.Next_state
		// reply.Y should be 100.
		// fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		// fmt.Printf("call failed!\n")
		// wr.state = 0
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	// fmt.Println("args, reply：", args, reply)

	err = c.Call(rpcname, args, reply)
	if err != nil {
		fmt.Println(err)
		return false
	}

	return true
}
