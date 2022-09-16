package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

type MasterTaskStatus int

const (
	Idle MasterTaskStatus = iota
	InProgress
	Completed
)

type MasterTask struct {
	TaskStatus    MasterTaskStatus
	StartTime     time.Time
	TaskReference *Task
}

type Task struct {
	Input         string
	TaskState     State
	NReducer      int
	TaskNumber    int
	Intermediates []string
	Output        string
}

type Coordinator struct {
	TaskQueue     chan *Task          // 等待执行的task
	TaskMeta      map[int]*MasterTask // 当前所有task的信息
	MasterPhase   State               // Master的阶段
	NReduce       int
	InputFiles    []string
	Intermediates [][]string // Map任务产生的R个中间文件的信息
	mu            sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) allTaskDone() bool {
	for _, task := range c.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) processTaskResult(task *Task) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch task.TaskState {
	case Map:
		//收集intermediate信息
		for reduceTaskId, filePath := range task.Intermediates {
			c.Intermediates[reduceTaskId] = append(c.Intermediates[reduceTaskId], filePath)
		}
		if c.allTaskDone() {
			//获得所以map task后，进入reduce阶段
			c.createReduceTask()
			c.MasterPhase = Reduce
		}
	case Reduce:
		if c.allTaskDone() {
			//获得所以reduce task后，进入exit阶段
			c.MasterPhase = Exit
		}
	}
}

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := c.MasterPhase == Exit
	return ret
}

func (c *Coordinator) TaskCompleted(task *Task, reply *ExampleReply) error {
	//更新task状态
	c.mu.Lock()
	if task.TaskState != c.MasterPhase || c.TaskMeta[task.TaskNumber].TaskStatus == Completed {
		// 因为worker写在同一个文件磁盘上，对于重复的结果要丢弃
		return nil
	}
	c.TaskMeta[task.TaskNumber].TaskStatus = Completed
	c.mu.Unlock()
	defer c.processTaskResult(task)
	return nil
}

func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	// assignTask就看看自己queue里面还有没有task
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.TaskQueue) > 0 {
		//有就发出去
		*reply = *<-c.TaskQueue
		// 记录task的启动时间
		c.TaskMeta[reply.TaskNumber].TaskStatus = InProgress
		c.TaskMeta[reply.TaskNumber].StartTime = time.Now()
	} else if c.MasterPhase == Exit {
		*reply = Task{TaskState: Exit}
	} else {
		// 没有task就让worker 等待
		*reply = Task{TaskState: Wait}
	}
	return nil
}

func (c *Coordinator) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		c.mu.Lock()
		if c.MasterPhase == Exit {
			c.mu.Unlock()
			return
		}
		for _, masterTask := range c.TaskMeta {
			if masterTask.TaskStatus == InProgress && time.Now().Sub(masterTask.StartTime) > 10*time.Second {
				c.TaskQueue <- masterTask.TaskReference
				masterTask.TaskStatus = Idle
			}
		}
		c.mu.Unlock()
	}
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

func (c *Coordinator) createMapTask() {
	nTask := len(c.InputFiles)
	for i := 0; i < nTask; i++ {
		task := Task{
			Input:         c.InputFiles[i],
			TaskState:     Map,
			NReducer:      c.NReduce,
			TaskNumber:    i,
			Intermediates: make([]string, 0),
		}
		masterTask := MasterTask{
			TaskReference: &task,
		}
		c.TaskMeta[i] = &masterTask
		c.TaskQueue <- &task
	}
}

func (c *Coordinator) createReduceTask() {
	nTask := c.NReduce
	taskMeta := make(map[int]*MasterTask, 0)
	for i := 0; i < nTask; i++ {
		task := Task{
			TaskState:     Reduce,
			TaskNumber:    i,
			Intermediates: c.Intermediates[i],
		}
		masterTask := MasterTask{
			TaskReference: &task,
		}
		taskMeta[i] = &masterTask
		c.TaskQueue <- &task
	}
	c.TaskMeta = taskMeta
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{
		TaskQueue:     make(chan *Task, max(nReduce, len(files))),
		TaskMeta:      make(map[int]*MasterTask),
		MasterPhase:   Map,
		NReduce:       nReduce,
		InputFiles:    files,
		Intermediates: make([][]string, nReduce),
		mu:            sync.Mutex{},
	}

	c.createMapTask()

	c.server()

	go c.catchTimeOut()

	return &c
}
