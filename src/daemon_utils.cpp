#include "daemon_utils.hpp"

#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>

namespace pgcdc 
{

bool write_pid_file(const std::string& pid_file_path) 
{
    std::ofstream f(pid_file_path, std::ios::trunc);
    if (!f.is_open()) {
        std::cerr << "warning: could not write PID file " << pid_file_path << "\n";
        return false;
    }
    f << getpid() << "\n";
    return true;
}

void remove_pid_file(const std::string& pid_file_path) 
{
    if (!pid_file_path.empty()) {
        ::unlink(pid_file_path.c_str());
    }
}

void daemonize(const std::string& pid_file_path) 
{
    // First fork: detach from the invoking shell's process group.
    pid_t pid = fork();
    if (pid < 0) {
        std::cerr << "daemonize: first fork failed\n";
        std::exit(1);
    }
    if (pid > 0) {
        // Original parent exits immediately — the shell that launched us
        // sees a normal, quick exit and gets its prompt back.
        std::exit(0);
    }

    // Child continues. Become session leader so we have no controlling
    // terminal at all.
    if (setsid() < 0) {
        std::cerr << "daemonize: setsid failed\n";
        std::exit(1);
    }

    // Second fork: prevents this process from ever reacquiring a
    // controlling terminal (only a session leader can do that).
    pid = fork();
    if (pid < 0) {
        std::cerr << "daemonize: second fork failed\n";
        std::exit(1);
    }
    if (pid > 0) {
        std::exit(0);
    }

    // We are now the daemon process, no controlling terminal, no parent
    // shell waiting on us.
    if (chdir("/") != 0) {
        // Non-fatal — just means relative paths could behave oddly if
        // anything in the codebase uses them, which nothing here does.
    }
    umask(0027);

    // Redirect stdio to /dev/null. Anything printed via std::cerr/cout
    // after this point goes nowhere — all real diagnostics from here on
    // must go through the file logger, which is initialized by the
    // caller immediately after this function returns.
    int devnull = open("/dev/null", O_RDWR);
    if (devnull >= 0) {
        dup2(devnull, STDIN_FILENO);
        dup2(devnull, STDOUT_FILENO);
        dup2(devnull, STDERR_FILENO);
        if (devnull > STDERR_FILENO) {
            close(devnull);
        }
    }

    if (!pid_file_path.empty()) {
        write_pid_file(pid_file_path);
    }
}

} // namespace pgcdc
