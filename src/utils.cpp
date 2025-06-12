
#include <vector>
#include <string>
#include <ctime>
#include <chrono>
#include <sstream>
#include <iomanip>

#include "fmt/format.h"

// Namespace for utility functions
namespace utils {
    unsigned long long get_time() {
        auto now = std::chrono::system_clock::now().time_since_epoch();
        return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
    }

    std::string format_time(std::time_t time) {
        std::stringstream ss;
        ss << std::put_time(std::gmtime(&time), "%F %X");
        return fmt::format("{}", ss.str());
    }

    void split_string(const std::string &str, char delim, std::vector<std::string> &out) {
        std::stringstream ss(str);
        std::string token;
        while (std::getline(ss, token, delim)) {
            out.push_back(token);
        }
    }

    std::string replace_all(std::string str, const std::string& from, const std::string& to) {
        size_t start_pos = 0;
        while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
            str.replace(start_pos, from.length(), to);
            start_pos += to.length(); // Move past the replacement
        }
        return str;
    }

}
