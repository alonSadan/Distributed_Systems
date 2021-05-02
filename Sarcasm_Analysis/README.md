# Sarcasm and Named Entities Analyser 

## Running instructions 

**prerequisites:** 
 - java 8 installed.
 - You should have aws account with enough credit.
 - You should have a credentials file with your credentials details and an active session.
 - You should have an active IAM role with the following permissions: 
_AmazonEC2FullAccess_, _AmazonSQSFullAccess_, _IAMFullAccess_, _AmazonS3FullAccess_.
   
Unzip the the file <file name> and run `java -jar local.jar input1 input2 ... inputN n [terminate]`.

## How it works

The app is built of three main parts: _Local_, _Manager_, _Worker_.  
_Local_ creates the manager(if it doesn't already exists) and send it the input files, 
for processing.  
The Manager gets the inputs, creates processing tasks and delegates them to the workers.  
The workers are created by the manager according to the input number `n` and the number of messages the manager receives from _Local_.  
_Manager_ then gathers all the answers from the workers and creates a html summery file,  
which _Local_ will download.  
In the following part there will be a detailed explanation on each part of the application:

### Queues used in the application

To coardinate between the different parts of the app aws SQS queues were used/
In our app we had blah queues:

- localSendQueue - Used by _Local_ to send messages to _Manager_.
- localReceiveQueue - Used by _Manager_ to send messages to _Local_.  
- Jobs - Used by _Manager_  to send jobs to the workers. 
- Answers - Used by workers to send processed reviews results to  _Manager_.

### Resources: 

- _Worker_: T2_MEDIUM, ami-01410adf56d01f03b 
- _Manager_: T2_MICRO, ami-0b9a720251e51c0fa

We chose to use T2_MEDIUM for the workers to handle the required processing power of the stanford library.
### Local flow

_Local_ uploads the inputs to S3, and then notifies _Manager_ (using localSendQueue).  
The message also contains the location of the input files.  
Then _Local_ busy-waits for _Manager_ to respond with the summery file location and downloads it.  
If _Local_ receives `terminate` as one of i's arguments it will terminate the running manager before exiting 
### Manager flow

The Manager main thread starts by receiving a message from _Local_ indicating the input files location.
Then, using a thread-pool, the _Manager_ creates two kinds of tasks: distribution and receive.    

In the distribution task the threads download the input file and parse it into single reviews.  
Then, the threads create m-k workers, where 'm' is the current required number of workers,  
and 'k' is the number of running workers. Last the threads will send setntiment and 
NER jobs to the workers.  

In the receive task the threads poll for answers from the workers, counts them, and
store the proccessed results.  
When all the jobs are done, they create the summery html file.

### Worker

Each worker endlessly poll for job, activates the appropriate processing 
algorithm - NER or sentiment analysis. When the process is done the worker will
send the answer to the answers queue.


## Security

We use IAM roles to allow the EC2 instances only the needed tasks.
Only a user connected to our personal aws account with our updated credentials file can run the app.

## Persistence

If a worker dies before finishing it's task, another worker will cover for it shortly, 
as the message will reappear in the jobs queue. Another worker will be created to replace it,
as part of the distribution task implementation.  





