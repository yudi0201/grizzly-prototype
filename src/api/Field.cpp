#include "api/Field.h"

Field::Field(std::string name, DataType type, std::size_t size, SourceType srcType, std::string default_value)
    : name(name), dataType(type), size(size), srcType(srcType), default_value(default_value) {}
