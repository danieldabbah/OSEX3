#include <csignal>
#include <iostream>
#include <atomic>
#include <set>
#include <algorithm>
#include "MapReduceClient.h"
#include "Barrier.h"
#include "MapReduceFramework.h"





//TODO: for each data type, check if we need to add mutex to it.



class Job;

using namespace std;
struct ThreadContext{
    int threadID;
    Job* p_job;
    IntermediateVec* p_personalThreadVector;
};

class Job{
    private:
        const int multiThreadLevel;
        pthread_t* threads;
        ThreadContext* threadContexts;
        IntermediateVec* p_personalThreadVectors;
        const MapReduceClient& client;
        const InputVec& inputVec;
        OutputVec* outputVec;
        atomic<uint64_t>* p_atomic_counter;        // 0-30 counter, 31-61 input size, 62-63 stage
        Barrier* p_afterSortBarrier;
        Barrier* p_afterShuffleBarrier;
        //TODO: add mutexes
        //TODO: in the destructor release the new
        pthread_mutex_t outputVectorMutex;
        vector<IntermediateVec*> intermediateVec;

    public:
        Job(const int multiThreadLevel,
            const MapReduceClient& client,
            const InputVec& inputVec, OutputVec& outputVec):
                outputVectorMutex(PTHREAD_MUTEX_INITIALIZER), intermediateVec(),
                multiThreadLevel(multiThreadLevel), client(client), inputVec(inputVec){
            outputVec = outputVec;
            this->threads = new pthread_t[multiThreadLevel];
            this->threadContexts = new ThreadContext[multiThreadLevel];
            this->p_personalThreadVectors = new IntermediateVec[multiThreadLevel];
            this->p_atomic_counter = new std::atomic<uint64_t>(inputVec.size() << 31);
            this->p_afterShuffleBarrier = new Barrier(multiThreadLevel);
            this->p_afterSortBarrier = new Barrier(multiThreadLevel);
        }



        virtual ~Job() {
            while (!this->intermediateVec.empty()){
                free(this->intermediateVec.back());
                this->intermediateVec.pop_back();
            }
            free(this->threads);
            free(this->threadContexts);
            free(this->p_personalThreadVectors);
            free(this->p_atomic_counter);
            free(this->p_afterShuffleBarrier);
            free(this->p_afterSortBarrier);
            if(pthread_mutex_destroy(&this->outputVectorMutex) != 0){
                //TODO: handle error
            }
        }

    //========================= get and set ======================================
        int getMultiThreadLevel() const {
            return multiThreadLevel;
        }

        vector<IntermediateVec*> &getIntermediateVec(){
            return intermediateVec;
        }

        Barrier *getPAfterShuffleBarrier() const {
            return p_afterShuffleBarrier;
        }

        pthread_mutex_t &getOutputVectorMutex() {
            return outputVectorMutex;
        }

        OutputVec* getOutputVector(){
            return this->outputVec;
        }

        IntermediateVec *getPpersonalVectors() const {
            return p_personalThreadVectors;
        }

        const InputVec &getInputVec() const {
            return inputVec;
        }

        const MapReduceClient &getClient() const {
            return client;
        }

        Barrier *getPAfterSortBarrier() const {
            return p_afterSortBarrier;
        }

    //========================= atomic counter ======================================

        unsigned long int addAtomicCounter(){
           return ((*this->p_atomic_counter)++) & (0x7fffffff);
        }

        void setAtomicCounterInputSize(unsigned long int size){
            (*this->p_atomic_counter) = ((*this->p_atomic_counter)&(~0x3fffffff80000000))|((unsigned long int)size << 31);
        }

        void addStageAtomicCounter(){
            (*this->p_atomic_counter) += ((unsigned long int)1 << 62);
        }

        void resetAtomicCounterCount(){
            *this->p_atomic_counter = (*this->p_atomic_counter) & (0xffffffff80000000);
        }

        unsigned long int getAtomicCounterCurrent(){
            return (this->p_atomic_counter->load()) & (0x7fffffff);
        }

        unsigned long int getAtomicCounterInputSize(){
            return ((this->p_atomic_counter->load()) >> 31) & (0x7fffffff);
        }

        stage_t getAtomicCounterState(){
            return stage_t((this->p_atomic_counter->load()) >> 62);
        }

        void setAtomicCounterStage(unsigned long int stage){
            *this->p_atomic_counter = ((*this->p_atomic_counter) & (0x3fffffffffffffff)) | (stage << 62);
        }
};


//========================= helpers to delete ======================================
void printVector2(vector<pair<K2*, V2*>> &vec, int vecId){
    cout<<endl;
    for (auto it = vec.begin(); it != vec.end(); it++){
        cout << "thread: " << vecId << " key:" ;
        it->first->print();
        cout << " value: ";
        it->second->print();
        cout << endl;
    }
}

void printVector3(vector<pair<K3*, V3*>> &vec, int vecId){
    cout<<endl;
    for (auto it = vec.begin(); it != vec.end(); it++){
        cout << "thread: " << vecId << " key:" ;
        it->first->print();
        cout << " value: ";
        it->second->print();
        cout << endl;
    }
}

//========================= map and sort phase ======================================

void emit2 (K2* key, V2* value, void* context){
    auto p_intermediateVec = (IntermediateVec*) context;
    p_intermediateVec->push_back(pair<K2*, V2*>(key, value));
}

bool cmpKeys(const IntermediatePair &a, const IntermediatePair &b){
    return *a.first < *b.first;
}

void mapAndSort(ThreadContext* tc){

    unsigned long int myIndex = 0;
    tc->p_job->setAtomicCounterStage(1);
    myIndex = tc->p_job->addAtomicCounter();     //the thread pick an index to work on:
    while (myIndex  < tc->p_job->getAtomicCounterInputSize()){
        // if the index is ok, perform the appropriate function of the client of the pair in the index.
        // get the matching pair from the index:
        InputPair inputPair = tc->p_job->getInputVec().at(myIndex);
        tc->p_job->getClient().map(inputPair.first,inputPair.second,tc->p_personalThreadVector);
        myIndex = tc->p_job->addAtomicCounter();
    }

    sort(tc->p_personalThreadVector->begin(), tc->p_personalThreadVector->end(), cmpKeys);
}

//========================= shuffle phase ======================================


bool isEqualKeys(const IntermediatePair &a, const IntermediatePair &b){
    return !cmpKeys(a,b) &&!cmpKeys(b,a);
}

/**
 * finds the index of the thread who has the largest key
 * @param tc thread context containing job
 * @return the index, -1 if the vectors are empty
 */
int findMaxKeyTid(ThreadContext* tc){
    K2 *maxKey = nullptr;
    int saveId = -1;
    vector<pair<K2*, V2*>>* curVector;
    for (int i = 0; i < tc->p_job->getMultiThreadLevel(); i++){
        curVector = &tc->p_job->getPpersonalVectors()[i];
        if (!curVector->empty()){
            if (maxKey == nullptr || *maxKey < *curVector->back().first) {
                maxKey = curVector->back().first;
                saveId = i;
            }
        }
    }
    return saveId;
}

void shuffle(ThreadContext* tc){ //TODO: advance the atomic counter after each phase counter and the phase itself. set the counter to 0 after each phase.
    unsigned long int outputSize = 0;
    for (int i = 0; i < tc->p_job->getMultiThreadLevel(); i++){
        outputSize += tc->p_job->getPpersonalVectors()[i].size();
    }
    tc->p_job->addStageAtomicCounter();
    tc->p_job->resetAtomicCounterCount();
    tc->p_job->setAtomicCounterInputSize(outputSize);
    unsigned long int count = 0;
    while(count <= outputSize){ // if count > outputsize, break
        int threadIdOfMax = findMaxKeyTid(tc); // find the index of the thread that contains the maximum key.
        IntermediateVec* p_currentVec = new IntermediateVec();
        IntermediatePair  currentPair = tc->p_job->getPpersonalVectors()[threadIdOfMax].back();
        // create intermidate vector
        for (int i = 0; i < tc->p_job->getMultiThreadLevel(); ++i) { // for every thread personal vector
            //TODO: personal vector can be empty
            while( !(tc->p_job->getPpersonalVectors()[i].empty())&&  isEqualKeys(tc->p_job->getPpersonalVectors()[i].back(), currentPair)){ //get all equal key pairs
                p_currentVec->push_back(tc->p_job->getPpersonalVectors()[i].back());
                tc->p_job->getPpersonalVectors()[i].pop_back();
                //TODO: need to update count only in the end or at each element
                count++;
            }
        }
        tc->p_job->getIntermediateVec().push_back(p_currentVec);
    }
}

//========================= reduce phase ======================================

void reduce(ThreadContext* tc){
    unsigned long int myIndex = 0;
    //the thread pick an index to work on:
    myIndex = tc->p_job->addAtomicCounter();
    //TODO: add call to reduce in the thread main function
    //TODO: add mutex before the insert to the output vector
    //TODO: change the size of the inputVector
    while (myIndex < tc->p_job->getAtomicCounterInputSize()){

        IntermediateVec* pairs = tc->p_job->getIntermediateVec().at(myIndex);

        tc->p_job->getClient().reduce(pairs,tc);
        myIndex = tc->p_job->addAtomicCounter();
    }
}

void emit3(K3* key,V3* value,void* context){
    ThreadContext* tc = (ThreadContext*) context;

    pthread_mutex_lock(&tc->p_job->getOutputVectorMutex());
    tc->p_job->getOutputVector()->push_back(pair<K3*, V3*>(key, value));
    pthread_mutex_unlock(&tc->p_job->getOutputVectorMutex());
}

//========================= main thread logic ======================================

void* threadMainFunction(void* arg)
{
    ThreadContext* tc = (ThreadContext*) arg;
    mapAndSort(tc);

    tc->p_job->getPAfterSortBarrier()->barrier();
    if (tc->threadID == 0) {
        shuffle(tc);
        tc->p_job->resetAtomicCounterCount();
        tc->p_job->addStageAtomicCounter();
        tc->p_job->setAtomicCounterInputSize(tc->p_job->getIntermediateVec().size());
    }
    tc->p_job->getPAfterShuffleBarrier()->barrier();
    reduce(tc);
    return nullptr;
}

void error_handler_function(const std::string& inputMessage){
    std::cerr<<"system error: "<<inputMessage<<std::endl;
    //TODO: free all allocate resorces
    exit(EXIT_FAILURE);
}

//========================= main exercise questions ======================================

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

    //For loop that init all thread contexts:
    for (int i = 0; i < multiThreadLevel; ++i) {
        threadContexts[i] = {i,job, &(job->getPpersonalVectors()[i])};
    }

    //For loop that create all threads:
    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_create(&threads[i], NULL,threadMainFunction , &threadContexts[i]);
    }
    //TODO: remove pthread join (it need to be in another function), in the 'waitForJon' function
    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_join(threads[i], NULL);
    }
    return static_cast<JobHandle> (job);
}

void getJobState(JobHandle job, JobState* state){
    //TODO: may return the wrong values if a context switch occurs in the middle of the deviation
    Job* p_job = static_cast<Job*>(job);
    state->stage = p_job->getAtomicCounterState();
    state->percentage = (100*(float)p_job->getAtomicCounterCurrent()) / (float)p_job->getAtomicCounterInputSize();
    if (state->percentage > 100.0) {
        state->percentage = 100.0;
    }
}




//    pthread_mutex_lock(tc->p_job->getMutexBeforeShuffle());
//    if (tc->p_job->isBeforeShuffle()){
//        if (tc->threadID == 0){
//            //perform shuffle
//            cout << "thread ID: " << tc->threadID << "  will preform a shuffle"<< endl;
//            tc->p_job->setBeforeShuffle(false);
//            pthread_cond_broadcast(tc->p_job->getCondBeforeShuffle());
//        } else {
//            cout << "thread ID: " << tc->threadID << "  gonna wait"<< endl;
//            pthread_cond_wait(tc->p_job->getCondBeforeShuffle(), tc->p_job->getMutexBeforeShuffle());
//            cout << "thread ID: " << tc->threadID << "  done waiting"<< endl;
//        }
//    }
//    pthread_mutex_unlock(tc->p_job->getMutexBeforeShuffle());
//    cout << "thread ID: " << tc->threadID << "  after mutex" << endl;


//
//    pthread_mutex_lock(&tc->p_job->getTestMutex());
//    printVector2(*tc->p_personalThreadVector, tc->threadID);
//    pthread_mutex_unlock(&tc->p_job->getTestMutex());