#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads), workers_(num_threads-1){
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    int remain_task = num_total_tasks;
    int num_one_issue = std::min(remain_task, (int)workers_.size());
    int cur_task = 0;
    while(remain_task > 0) {
        for(int i = 1; i < num_one_issue; ++i) {
            workers_[i-1] = std::thread(&IRunnable::runTask, runnable, cur_task+i, num_total_tasks);
        }
        runnable->runTask(cur_task, num_total_tasks);
        for (int i = 1; i < num_one_issue; i++) {
           workers_[i-1].join();
        }
        cur_task += num_one_issue;
        remain_task -= num_one_issue;
    } while(remain_task > 0);
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads), is_shut_down_(false), thread_pools_(num_threads) {
    for(int i = 0; i < num_threads; ++i) {
        thread_pools_[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::threadEntry, this, i);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    is_shut_down_ = true;
    for(auto & t : thread_pools_) {
        t.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (int i = 0; i < num_total_tasks; i++) {
            task_queue_.push(std::bind(&IRunnable::runTask, runnable, i, num_total_tasks));
        }
        num_completed_tasks_ = 0;
    }

    while(true) {
        std::lock_guard<std::mutex> lock(mu_);
        if(num_completed_tasks_ == num_total_tasks) {
            break;
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::threadEntry(int thread_id) {
    while(!is_shut_down_) {
        std::function<void(void)> task;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if(!task_queue_.size()) continue;
            task = task_queue_.front();
            task_queue_.pop();
        }
        task();
        {
            std::lock_guard<std::mutex> lock(mu_);
            ++num_completed_tasks_;
        }
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), is_shut_down_(false), thread_pool_(num_threads) {
    for(int i = 0; i < num_threads; ++i) {
        thread_pool_[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::threadEntry, this, i);
    }

}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    is_shut_down_ = true;
    new_task_cond_.notify_all();
    for(auto & t : thread_pool_) t.join();
}

void TaskSystemParallelThreadPoolSleeping::threadEntry(int thread_id) {
    while(!is_shut_down_) {
        std::function<void(void)> task;
        {
            std::unique_lock<std::mutex> lock(mu_);
            new_task_cond_.wait(lock, [this]{return this->is_shut_down_ || this->task_queue_.size();});
            
            if(!task_queue_.size()) continue;

            task = task_queue_.front();
            task_queue_.pop();
        }
        task();
        {
            std::lock_guard<std::mutex> lock(mu_);
            ++num_completed_threads_;
        }
        task_done_cond_.notify_one();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    num_completed_threads_ = 0;
    std::unique_lock<std::mutex> lock(mu_);
    for(int i = 0; i < num_total_tasks; ++i) {
        task_queue_.push(std::bind(&IRunnable::runTask, runnable, i, num_total_tasks));
    }
    new_task_cond_.notify_all();
    
    task_done_cond_.wait(lock, [this, num_total_tasks]{return this->num_completed_threads_ == num_total_tasks;});
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
