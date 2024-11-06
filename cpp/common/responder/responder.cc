
#include "common/responder/responder.h"

#include <utility>

#include "utils/logging/logging.h"

namespace runai::llm::streamer::common
{

Responder::Responder(unsigned running) :
    _running(running),
    _ready(0),
    _total_bytesize(0),
    _start_time(std::chrono::steady_clock::now())
{
    LOG(DEBUG) << "created responder for " << running << " running requests";
}

Responder::~Responder()
{
    LOG(DEBUG) << "Responder shutdown";
}

// return -1 if there are no running requests
Response Responder::pop()
{
    if (_stopped || finished())
    {
        LOG(DEBUG) << (_stopped ? "responder does not expect any more responses" : "responder stopped");
        return ResponseCode::FinishedError;
    }

    _ready.wait();

    const auto guard = std::unique_lock<std::mutex>(_mutex);

    if (_stopped)
    {
        LOG(DEBUG) << "responder stopped";
        return ResponseCode::FinishedError;
    }

    ASSERT(!_responses.empty()) << "responder is empty after notification. Current running " << _running;

    auto response = _responses.front();
    LOG(SPAM) << "Sending response: " << response;
    _responses.pop_front();

    return response;
}

void Responder::push(Response && response)
{
    {
        const auto guard = std::unique_lock<std::mutex>(_mutex);

        _successful  = _successful && response.ret == common::ResponseCode::Success;

        if (_running)
        {
            LOG(SPAM) << response << " ; " << _running << " running requests";
            _responses.push_back(response);
            --_running;

            if (_running == 0 && _successful && _total_bytesize > 100 * 1024 * 1024)
            {
                const auto throughput = bytes_per_second();
                std::cout << "Read throughput is " << utils::logging::human_readable_size(throughput) << " per second " << std::endl;
            }
        }
        else
        {
            LOG(ERROR) << "Received unexpected response (no running requests) " << response;
            _responses.push_back(ResponseCode::UnknownError);
        }
    }
    _ready.post();
}

void Responder::push(Response && response, size_t bytesize)
{
    _total_bytesize += bytesize;
    push(std::move(response));
}


bool Responder::finished() const
{
    const auto guard = std::unique_lock<std::mutex>(_mutex);
    return (_canceled || (_running == 0 && _responses.empty()));
}

void Responder::cancel()
{
    const auto guard = std::unique_lock<std::mutex>(_mutex);
    _canceled = true;
}

void Responder::stop()
{
    {
        const auto guard = std::unique_lock<std::mutex>(_mutex);
        _stopped = true;
    }
    // wake up blocking waiting threads
    _ready.post();
}

size_t Responder::bytes_per_second() const
{
    const auto time_ = std::chrono::steady_clock::now();
    const auto duration  = std::chrono::duration_cast<std::chrono::milliseconds>(time_ - _start_time);
    const auto milliseconds = duration.count();

    LOG(DEBUG) << "Read " << utils::logging::human_readable_size(_total_bytesize) << " in " << milliseconds << " milliseconds";
    return static_cast<double>(_total_bytesize) / (static_cast<double>(milliseconds) / 1000.0);
}

}; // namespace runai::llm::streamer::common
