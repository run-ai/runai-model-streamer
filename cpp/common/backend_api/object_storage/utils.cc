#include "common/backend_api/object_storage/utils.h"

namespace runai::llm::streamer::common::backend_api
{

std::ostream & operator<<(std::ostream & os, const ObjectConfigParam_t & param)
{
    os << param.key << " : " << param.value;
    return os;
}

std::ostream & operator<<(std::ostream & os, const ObjectClientConfig_t & config)
{
    os << "endpoint_url: " << config.endpoint_url << ", num_initial_params: " << config.num_initial_params << ", initial_params: ";
    for (unsigned i = 0; i < config.num_initial_params; ++i)
    {
        os << " " << config.initial_params[i] << ", ";
    }
    return os;
}

} // namespace runai::llm::streamer::common::backend_api
