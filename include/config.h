// Created by: gtoscano
#ifndef CONFIG_H
#define CONFIG_H

#include <string>
namespace config {
    std::string get_env_var(const std::string& key, const std::string& default_value = "");
}

#endif
