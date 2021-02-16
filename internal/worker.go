package internal

import (
	"encoding/json"
	"fmt"
)

type ReqType string
type ReqAction string

const (
	NEW     ReqAction = "NEW"
	CANCEL            = "CANCEL"
	REPLACE           = "REPLACE"
)

type Payload struct {
	ID     string          `json:"id"`
	Type   ReqType         `json:"type"`
	Action ReqAction       `json:"action"`
	Data   json.RawMessage `json:"data"`
}

// Response struct{}
type Response struct {
	ErrorCode    int32
	ErrorMessage *string
	Payload      *Payload
}

// Job represents the job to be run
type Job struct {
	Payload Payload
}

// JobQueue is a buffered channel that we can send work requests on.
var JobQueue = make(chan Job)

// Worker represents the worker that executes the job
type Worker struct {
	WorkerPool chan chan Job
	JobChannel chan Job
	quit       chan bool
}

func NewWorker(workerPool chan chan Job) Worker {
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool)}
}

// Start method starts the run loop for the worker,
// listening for a quit channel in case we need to stop it
func (w Worker) Start() {
	go func() {
		for {
			// register the current worker into the worker queue.
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:

				// we have received a work request.
				fmt.Printf("%% Process Job id=%s :: type=%s\n",
					string(job.Payload.ID), string(job.Payload.Type))

				// Given the job.Payload.Type and job.Payload.Action
				// Process accordingly!
				// Example:
				// func() { // wrap in func for defer reply

				// 	requestError := ""
				// 	var payload *Payload

				// 	// make sure reply message sent in the end
				// 	defer func() {
				// 		var data []byte
				// 		errCode := int32(0)
				// 		if requestError != "" {
				// 			errCode = int32(-1)
				// 		}
				// 		switch job.Payload.Action {
				// 		case NEW:
				// 			resp := &Response{
				// 				ErrorCode:    errCode,
				// 				ErrorMessage: &requestError,
				// 				Payload:      payload,
				// 			}
				// 			data = []byte(fmt.Sprintf("%v", resp))
				// 		case CANCEL:
				// 			resp := &Response{
				// 				ErrorCode:    errCode,
				// 				ErrorMessage: &requestError,
				// 				Payload:      payload,
				// 			}
				// 			data = []byte(fmt.Sprintf("%v", resp))
				// 		case REPLACE:
				// 			resp := &Response{
				// 				ErrorCode:    errCode,
				// 				ErrorMessage: &requestError,
				// 				Payload:      payload,
				// 			}
				// 			data = []byte(fmt.Sprintf("%v", resp))
				// 		}
				// 		fmt.Printf("%% Response=%s\n", string(data))
				// 		// Publish response back to kafka e.g. KafkaPublishResp(broker, data)
				// 	}()
				// }()

				// Persist order entry to another store

			case <-w.quit:
				// we have received a signal to stop
				return
			}
		}
	}()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	fmt.Println("\nWorkers stopped")
	go func() {
		w.quit <- true
	}()
}
