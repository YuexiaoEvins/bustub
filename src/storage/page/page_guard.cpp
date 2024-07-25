#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
    this->bpm_ = that.bpm_;
    this->page_ = that.page_;
    this->is_dirty_ = that.is_dirty_;
    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
    if (page_ != nullptr){
      bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    }
    this->page_ = nullptr;
    this->bpm_ = nullptr;
    this->is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
    this->Drop();
    this->bpm_ = that.bpm_;
    this->page_ = that.page_;
    this->is_dirty_ = that.is_dirty_;
    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
    return *this;
}

BasicPageGuard::~BasicPageGuard(){
    this->Drop();
};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
    this->page_->RLatch();
    ReadPageGuard rpg(this->bpm_,this->page_);
    this->bpm_ = nullptr;
    this->page_ = nullptr;
    this->is_dirty_ = false;
    return rpg;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
    this->page_->WLatch();
    WritePageGuard rpg(this->bpm_,this->page_);
    this->bpm_ = nullptr;
    this->page_ = nullptr;
    this->is_dirty_ = false;
    return rpg;
}

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) {}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
    this->guard_ = std::move(that.guard_);
};

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
    this->Drop();
    this->guard_ = std::move(that.guard_);
    return *this;
}

void ReadPageGuard::Drop() {
    auto &basic_guard = this->guard_;
    if (basic_guard.page_ != nullptr) {
      basic_guard.bpm_->UnpinPage(basic_guard.page_->GetPageId(), basic_guard.is_dirty_);
      basic_guard.page_->RUnlatch();
    }
    guard_.page_ = nullptr;
    guard_.bpm_ = nullptr;
    guard_.is_dirty_ = false;
}

ReadPageGuard::~ReadPageGuard() {
    this->Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) {}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
    this->guard_ = std::move(that.guard_);
};

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
    this->Drop();
    this->guard_ = std::move(that.guard_);
    return *this;
}

void WritePageGuard::Drop() {
    auto &basic_guard = this->guard_;
    if (basic_guard.page_ != nullptr) {
      basic_guard.bpm_->UnpinPage(basic_guard.page_->GetPageId(), basic_guard.is_dirty_);
      basic_guard.page_->WUnlatch();
    }
    guard_.page_ = nullptr;
    guard_.bpm_ = nullptr;
    guard_.is_dirty_ = false;
}

WritePageGuard::~WritePageGuard() {
    this->Drop();
}  // NOLINT

}  // namespace bustub
