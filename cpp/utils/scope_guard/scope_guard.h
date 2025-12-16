#pragma once

#include <functional>

namespace runai::llm::streamer::utils
{

struct ScopeGuard
{
    ScopeGuard(std::function<void()> && lambda);

    ScopeGuard(const ScopeGuard&) = delete;
    ScopeGuard& operator=(const ScopeGuard&) = delete;

    ScopeGuard(ScopeGuard && other);
    ScopeGuard& operator=(ScopeGuard&& other);

    ~ScopeGuard();

    void cancel();

 private:
    void _release();

    std::function<void()> _lambda;
};

} // namespace runai::llm::streamer::utils
