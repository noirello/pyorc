#ifndef USE_GUARD_H
#define USE_GUARD_H

#include <atomic>
#include <pybind11/pybind11.h>

namespace py = pybind11;

#ifdef Py_GIL_DISABLED

class UseFlag
{
  private:
    std::atomic<bool> busy{ false };

  public:
    class Guard
    {
      private:
        UseFlag& flag;

      public:
        explicit Guard(UseFlag& f)
          : flag(f)
        {
            if (flag.busy.exchange(true, std::memory_order_acquire)) {
                throw std::runtime_error(
                  "Concurrent use of the same ORC reader/writer (or of readers sharing "
                  "one file object) is not supported");
            }
        }
        ~Guard() { flag.busy.store(false, std::memory_order_release); }
        Guard(const Guard&) = delete;
        Guard& operator=(const Guard&) = delete;
    };
};

#else
// GIL build: no-op
class UseFlag
{
  public:
    class Guard
    {
      public:
        explicit Guard(UseFlag&) {}
    };
};

#endif

#endif