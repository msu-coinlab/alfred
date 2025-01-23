// Created by: gtoscano
#include <string>
#include <cstdlib>

namespace config {
    std::string get_env_var(const std::string& key, const std::string& default_value = "") {
        const char* val = std::getenv(key.c_str());
        return val ? std::string(val) : default_value;
    }
}
