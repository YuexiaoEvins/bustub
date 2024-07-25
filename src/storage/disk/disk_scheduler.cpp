//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
//  throw NotImplementedException(
//      "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the "
//      "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  //  Schedules a request for the DiskManager to execute.
  //  The DiskRequest struct specifies whether the request is for a read/write,
  //  where the data should be written into/from, and the page ID for the operation.
  //  The DiskRequest also includes a std::promise whose value should be set to true once the request is processed.

  request_queue_.Put(std::move(r));
}

void DiskScheduler::StartWorkerThread() {
  //  Start method for the background worker thread which processes the scheduled requests.
  //  The worker thread is created in the DiskScheduler constructor and calls this method.
  //  This method is responsible for getting queued requests and dispatching them to the DiskManager.
  //  Remember to set the value on the DiskRequest's callback to signal to the request issuer that the request has been completed.
  //  This should not return until the DiskScheduler's destructor is called.

  std::optional<DiskRequest> request;
  while ((request = request_queue_.Get())) {
    if (request) {
      if (request->is_write_) {
        disk_manager_->WritePage(request->page_id_,request->data_);
        request->callback_.set_value(true);
        continue ;
      }
      disk_manager_->ReadPage(request->page_id_,request->data_);
      request->callback_.set_value(true);
    }
  }


}

}  // namespace bustub
