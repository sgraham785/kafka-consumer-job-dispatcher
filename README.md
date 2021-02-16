# Kafka Consumer Job Dispatcher Sample

This sample was created because of a botched interview. Being the interviewee I missed the oppotunity. The feedback was "...hard to tell if he actually knows his stuff and can provide the output we need. We'll have to pass..."

While I would like to justify my high level explainations. I thought it better to create this sample of processing topics off of Kafka with a dispatcher worker pattern with channels, which is the most relavent to the opportunity missed. 

With this sample you can "plug-in" different types of message busses and pass the work to the `JobQueue` channel. As well as, setup how you would like to process the data. Right now it lightly structures the requirements to have a type and action field which you can switch from to process work differently.

## How to

- Pull this project and into it.
- Run `docker-compose up -d`
- In a terminal `go run cmd/main.go <topic_name>`
- In another terminal publish data to `<topic_name>` using something like [kafkacat](https://github.com/edenhill/kafkacat)
- You will see the workers process work
  
## Options
```
go run cmd/main.go -h
Usage:
  -b string
        Address to consumer broker (default "localhost:9092")
  -g string
        Consumer group.id (default "tester")
  -w int
        Max number of workers (default 2)
```