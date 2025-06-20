// Created by: gtoscano 
#ifndef UTILITIES_H
#define UTILITIES_H

#include <vector>
#include <string>
#include <ctime>
#include <chrono>


namespace utils {
    unsigned long long get_time();
    std::string format_time(std::time_t time);
    void split_string(const std::string &str, char delim, std::vector<std::string> &out);
    std::string replace_all(std::string str, const std::string& from, const std::string& to);  // Add this line
}
#endif
