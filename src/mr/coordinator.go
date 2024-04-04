package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type map_item struct {
	map_id        int
	map_task_list []string
}

type reduce_item struct {
	reduce_id        int
	reduce_task_list []string
}

type Coordinator struct {
	// Your definitions here.
	map_tasks              []map_item
	task_queue             []int
	allocated_map_tasks    map[int]int64
	map_tasks_ch           []chan bool
	allocated_reduce_tasks map[int]int64
	reduce_tasks_ch        []chan bool
	//0 init  1 map  2 reduce
	state        int
	output_files []string
	mu           sync.Mutex
	nreduce      int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// reply.Y = args.X + 1

	if c.state == 0 {
		// fmt.Println("coordinator change state", 1)
		c.state = 1

		for k := range c.allocated_map_tasks {
			delete(c.allocated_map_tasks, k)
		}

		c.task_queue = c.task_queue[:0]

		for _, ch := range c.map_tasks_ch {
			close(ch)
		}
		c.map_tasks_ch = c.map_tasks_ch[:0]

		for _, map_task := range c.map_tasks {
			c.task_queue = append(c.task_queue, map_task.map_id)
			c.map_tasks_ch = append(c.map_tasks_ch, make(chan bool))
			c.allocated_map_tasks[map_task.map_id] = -1
		}
	}

	// fmt.Println("此worker状态：", args.State)
	if args.State == 0 {

	} else if args.State == 1 {
		//注意拒收超时worker的请求
		if c.allocated_map_tasks[args.Map_id] == args.worker_id {
			c.map_tasks_ch[args.Map_id] <- true
		}
	} else if args.State == 2 {
		if c.allocated_reduce_tasks[args.Reduce_id] == args.worker_id {
			c.reduce_tasks_ch[args.Reduce_id] <- true
		}
	}

	//初始化reduce任务
	if c.state == 1 && len(c.allocated_map_tasks) == 0 && len(c.task_queue) == 0 {
		// fmt.Println("coordinator change state", 2)
		c.state = 2

		for k := range c.allocated_reduce_tasks {
			delete(c.allocated_reduce_tasks, k)
		}

		for _, ch := range c.reduce_tasks_ch {
			close(ch)
		}
		c.reduce_tasks_ch = c.reduce_tasks_ch[:0]

		for i := 0; i < c.nreduce; i++ {
			c.task_queue = append(c.task_queue, i)
			c.reduce_tasks_ch = append(c.reduce_tasks_ch, make(chan bool))
			c.allocated_reduce_tasks[i] = -1
		}
	}

	//结束任务
	if c.state == 2 && len(c.allocated_reduce_tasks) == 0 && len(c.task_queue) == 0 {
		// fmt.Println("coordinator change state", 3)
		c.state = 3
		//汇总输出文件
		// pattern := "mr-out-\\d+"
		// matcher := regexp.MustCompile(pattern)

		// pwd, pwderr := os.Getwd()

		// if pwderr != nil {
		// 	fmt.Println("Error Getwd:", pwderr)
		// }

		// files := make([]string, 0)
		// err := filepath.Walk(pwd, func(path string, info os.FileInfo, err error) error {
		// 	if err != nil {
		// 		return err
		// 	}
		// 	if !info.IsDir() && matcher.MatchString(info.Name()) {
		// 		files = append(files, info.Name())
		// 	}
		// 	return nil
		// })

		// if err != nil {
		// 	fmt.Println("Error walking the path:", err)
		// }
	}

	// fmt.Println("当前arg：", args)

	// fmt.Println("map任务剩余：", len(c.map_task))

	//分配任务
	if c.state == 0 {

	} else if c.state == 1 {
		//分配map任务
		if len(c.task_queue) == 0 {
			//让worker等待
			reply.Next_state = 0
		} else {
			curtask := c.task_queue[0]
			c.task_queue = c.task_queue[1:]
			reply.Next_state = 1
			reply.Map_id = curtask
			reply.Reduce_id = -1
			reply.Nreduce = c.nreduce
			reply.Files_list = c.map_tasks[curtask].map_task_list

			c.allocated_map_tasks[curtask] = args.worker_id
			//超时处理
			go func(map_id int) {
				select {
				case <-c.map_tasks_ch[map_id]:
					//完成任务的处理操作要放在这里面，否则可能执行了任务完成处理，但是已经触发了超时导致错误
					c.mu.Lock()
					//通过删除标记为已完成
					delete(c.allocated_map_tasks, map_id)
					c.mu.Unlock()
				case <-time.After(10 * time.Second):
					c.mu.Lock()
					c.allocated_map_tasks[map_id] = -1
					c.task_queue = append(c.task_queue, map_id)
					c.mu.Unlock()
				}
			}(curtask)
		}
	} else if c.state == 2 {
		//分配reduce任务
		if len(c.task_queue) == 0 {
			//让worker等待
			reply.Next_state = 0
		} else {
			curtask := c.task_queue[0]
			c.task_queue = c.task_queue[1:]
			reply.Next_state = 2
			reply.Map_id = -1
			reply.Reduce_id = curtask
			reply.Nreduce = c.nreduce

			c.allocated_reduce_tasks[curtask] = args.worker_id
			//超时处理
			go func(reduce_id int) {
				select {
				case <-c.reduce_tasks_ch[reduce_id]:
					//完成任务的处理操作要放在这里面，否则可能执行了任务完成处理，但是已经触发了超时导致错误
					c.mu.Lock()
					//通过删除标记为已完成
					delete(c.allocated_reduce_tasks, reduce_id)
					c.mu.Unlock()
				case <-time.After(10 * time.Second):
					c.mu.Lock()
					c.allocated_reduce_tasks[reduce_id] = -1
					c.task_queue = append(c.task_queue, reduce_id)
					c.mu.Unlock()
				}
			}(curtask)
		}
	} else if c.state == 3 {
		//结束
		reply.Next_state = 3
	}

	// reply.Y = 2
	// fmt.Println("当前reply", reply)

	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := false

	// Your code here.
	ret = c.state == 3

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{make([]map_item, 0), make([]int, 0), make(map[int]int64), make([]chan bool, 0),
		make(map[int]int64), make([]chan bool, 0), 0, make([]string, 0), sync.Mutex{}, nReduce}

	//初始化map任务
	for i, file := range files {
		c.map_tasks = append(c.map_tasks, map_item{i, []string{file}})
	}

	// Your code here.

	c.server()
	// fmt.Println("server have started!")
	return &c
}
