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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

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
    int remain_task = num_total_tasks;
    int cur_task = 0;
    while(remain_task > 0) {
        int num_one_issue = std::min(remain_task, (int)workers_.size());

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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), is_shut_down_(false), next_launch_id_(0), num_outstanding_launches_(0), thread_pool_(num_threads) {
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
        Task task;
        {
            std::unique_lock<std::mutex> lock(mu_);
            new_task_cond_.wait(lock, [this]{return this->is_shut_down_ || this->ready_task_queue_.size();});
            
            if(!ready_task_queue_.size()) continue;

            task = ready_task_queue_.front();
            ready_task_queue_.pop();
        }
        task.func();
        {
            std::lock_guard<std::mutex> lock(mu_);
            if(!--num_outstanding_tasks_[task.launch_id]) {
                --num_outstanding_launches_;
                launch_done_cond_.notify_one();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    std::lock_guard<std::mutex> lock(mu_);
    ++num_outstanding_launches_;
    num_outstanding_tasks_[next_launch_id_] = num_total_tasks;
    if(isDependencyResolve(deps)) {
        for(int i = 0; i < num_total_tasks; ++i) {
           ready_task_queue_.push({next_launch_id_, std::bind(&IRunnable::runTask, runnable, i, num_total_tasks)});
        }
        new_task_cond_.notify_all();
    } else {
        waiting_launch_queue_.push_back({next_launch_id_, runnable, num_total_tasks, deps});
    }
    return next_launch_id_++;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lock(mu_);
    while(!waiting_launch_queue_.empty()) {
        for(auto it = waiting_launch_queue_.begin(); it != waiting_launch_queue_.end();) {
            const auto &launch = *it; 
            const auto & [launch_id, runnable, num_total_tasks, deps] = launch;
            if(!isDependencyResolve(deps)) {
                ++it; continue;
            }
            for(int i = 0; i < num_total_tasks; ++i) {
                ready_task_queue_.push({launch_id, std::bind(&IRunnable::runTask, runnable, i, num_total_tasks)});
            }
            it = waiting_launch_queue_.erase(it);
        }
        new_task_cond_.notify_all();
        launch_done_cond_.wait(lock);
    }
    launch_done_cond_.wait(lock, [this]{return !this->num_outstanding_launches_;});
    // num_outstanding_tasks_.clear();
}

bool TaskSystemParallelThreadPoolSleeping::isDependencyResolve(const std::vector<TaskID>& deps) {
    for(auto launch_id : deps) {
        if(num_outstanding_tasks_[launch_id]) return false;
    }
    return true;
}