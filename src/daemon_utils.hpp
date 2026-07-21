#pragma once
#include <string>

namespace pgcdc 
{

    void daemonize(const std::string& pid_file_path);
    bool write_pid_file(const std::string& pid_file_path);
    void remove_pid_file(const std::string& pid_file_path);

} // namespace pgcdc
