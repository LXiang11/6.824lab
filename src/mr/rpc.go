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
	X         int
	Map_id    int
	Reduce_id int
	State     int
	worker_id int64
	//中间文件名
	In_file []string
	//输出文件名
	Output_file string
}

type ExampleReply struct {
	Y          int
	Next_state int
	Map_id     int
	Reduce_id  int
	Nreduce    int
	Files_list []string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
