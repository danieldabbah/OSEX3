#include <csignal>
#include <iostream>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
struct ThreadContext{
    int threadID;
    int startIndex;
    int endIndex;

};

class Job{
public:
    Job(const int multiThreadLevel,
        const MapReduceClient& client,
        const InputVec& inputVec, OutputVec& outputVec):
            multiThreadLevel(multiThreadLevel),
            client(client),
            inputVec(inputVec)


            {
        //TODO: check if new command fail
        outputVec = outputVec;

         threads = new pthread_t[multiThreadLevel];
        threadContexts = new ThreadContext[multiThreadLevel];
        this->state = {UNDEFINED_STAGE,0};

    }
private:
    const int multiThreadLevel;
    pthread_t* threads;
    ThreadContext* threadContexts;
    const MapReduceClient& client;
    const InputVec inputVec;
    OutputVec* outputVec;
    JobState state;
    //TODO: add mutexes

//TODO: in the destructor relase the new
};





// TODO: create a thread context class/ struct that have thread id, and relavent data
//TODO: create a job class
// TODO: create the "main" function for each thread
//TODO: add to the job class all the init of the input vector, interminate vector, output vector
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    // allocate a job class on the heap
    Job* job = new Job(multiThreadLevel,client,inputVec,outputVec);
    // create all the threads:
    pthread_t threads[multiThreadLevel];
    ThreadContext threadContexts[multiThreadLevel];
    // set the job class to have access to the thread pointer


    return static_cast<JobHandle> (job);




}
