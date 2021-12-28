#include "PyORCStream.h"

PyORCInputStream::PyORCInputStream(py::object fp)
{
    if (!(py::hasattr(fp, "read") && py::hasattr(fp, "seek"))) {
        throw py::type_error("Parameter must be a file-like object, but `" +
                             (std::string)(py::str(fp.get_type())) + "` was provided");
    }
    pyread = fp.attr("read");
    pyseek = fp.attr("seek");
    py::object isSeekable(fp.attr("seekable"));
    if (py::cast<bool>(isSeekable()) == false) {
        throw py::type_error("File-like object must be seekable");
    }
    if (py::hasattr(fp, "name")) {
        filename = py::cast<std::string>(py::str(fp.attr("name")));
    } else {
        filename = py::cast<std::string>(py::repr(fp));
    }
    py::object pytell(fp.attr("tell"));
    uint64_t currPos = py::cast<uint64_t>(pytell());
    totalLength = py::cast<uint64_t>(pyseek(0, 2));
    pyseek(currPos);
}

uint64_t
PyORCInputStream::getLength() const
{
    return totalLength;
}

uint64_t
PyORCInputStream::getNaturalReadSize() const
{
    return 128 * 1024;
}

const std::string&
PyORCInputStream::getName() const
{
    return filename;
}

void
PyORCInputStream::read(void* buf, uint64_t length, uint64_t offset)
{
    char* src;
    Py_ssize_t bytesRead;
    if (!buf) {
        throw orc::ParseError("Buffer is null");
    }

    pyseek(offset);
    py::object data = pyread(length);
    int rc = PyBytes_AsStringAndSize(data.ptr(), &src, &bytesRead);
    if (rc == -1) {
        PyErr_Clear();
        throw orc::ParseError(
          "Failed to read content as bytes. Stream might not be opened as binary");
    }

    if (static_cast<uint64_t>(bytesRead) != length) {
        throw orc::ParseError("Short read of " + filename);
    }

    std::memcpy(buf, src, length);
}

PyORCInputStream::~PyORCInputStream() {}

PyORCOutputStream::PyORCOutputStream(py::object fp)
{
    bytesWritten = 0;
    if (!(py::hasattr(fp, "write") && py::hasattr(fp, "flush"))) {
        throw py::type_error("Parameter must be a file-like object, but `" +
                             (std::string)(py::str(fp.get_type())) + "` was provided");
    }
    pywrite = fp.attr("write");
    pyflush = fp.attr("flush");
    if (py::hasattr(fp, "name")) {
        filename = py::cast<std::string>(py::str(fp.attr("name")));
    } else {
        filename = py::cast<std::string>(py::repr(fp));
    }
    closed = py::cast<bool>(fp.attr("closed"));
}

uint64_t
PyORCOutputStream::getLength() const
{
    return bytesWritten;
}

uint64_t
PyORCOutputStream::getNaturalWriteSize() const
{
    return 128 * 1024;
}

const std::string&
PyORCOutputStream::getName() const
{
    return filename;
}

void
PyORCOutputStream::write(const void* buf, size_t length)
{
    if (closed) {
        throw std::logic_error("Cannot write to closed stream");
    }
    try {
        py::bytes data = py::bytes(static_cast<const char*>(buf), length);
        size_t count = py::cast<size_t>(pywrite(data));
        pyflush();

        if (count != length) {
            throw orc::ParseError("Shorter write of " + filename);
        }
        bytesWritten += static_cast<uint64_t>(count);
    } catch (py::error_already_set& err) {
        if (!err.matches(PyExc_TypeError)) {
            throw;
        }
        throw orc::ParseError(
          "Failed to write content as bytes. Stream might not be opened as binary");
    }
}

void
PyORCOutputStream::close()
{
    if (!closed) {
        try {
            pyflush();
        } catch (py::error_already_set& err) {
            if (!err.matches(PyExc_ValueError)) {
                throw;
            }
            // ValueError is raised when try to flush on a closed file, let's ignore.
            PyErr_Clear();
        }
        closed = true;
    }
}

PyORCOutputStream::~PyORCOutputStream()
{
    close();
}
