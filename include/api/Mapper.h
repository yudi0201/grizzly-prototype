#ifndef API_MAPPER_H
#define API_MAPPER_H

#include "api/Field.h"
#include "operator/Operator.h"
#include <sstream>

class Mapper {
public:
  // string value
  Mapper(std::string fieldId, std::string value, std::string outputField)
      : fieldId(fieldId), value("\"" + value + "\""), outputField(outputField) {}
  Mapper(std::string fieldId, std::string value) : Mapper(fieldId, value, fieldId) {}
  // long value
  Mapper(std::string fieldId, long value, std::string outputField)
      : fieldId(fieldId), value(std::to_string(value)), outputField(outputField) {}
  Mapper(std::string fieldId, long value) : Mapper(fieldId, value, fieldId) {}
  // field value
  Mapper(std::string fieldId, Field &value, std::string outputField)
      : fieldId(fieldId), value("record." + value.name), outputField(outputField) {}
  Mapper(std::string fieldId, Field &value) : Mapper(fieldId, value, fieldId) {}

  std::string to_string() { return "Map Field: " + fieldId + "  on " + value + " with " + outputField; }

  virtual void produce(CodeGenerator &cg, Operator *input) {
    pipeline = cg.currentPipeline();
    input->produce(cg);
  };
  virtual void consume(CodeGenerator &cg, Operator *parent) = 0;
  size_t pipeline;

protected:
  std::string fieldId;
  std::string value;
  std::string outputField;
};

// adds a value to a field or adds two fields
class Add : public Mapper {
public:
  Add(std::string fieldId, std::string value) : Mapper(fieldId, value) {}
  Add(std::string fieldId, std::string value, std::string outputField) : Mapper(fieldId, value, outputField) {}
  Add(std::string fieldId, long value) : Mapper(fieldId, value) {}
  Add(std::string fieldId, long value, std::string outputField) : Mapper(fieldId, value, outputField) {}
  Add(std::string fieldId, Field &value) : Mapper(fieldId, value) {}
  Add(std::string fieldId, Field &value, std::string outputField) : Mapper(fieldId, value, outputField) {}

  void consume(CodeGenerator &cg, Operator *parent) {
    std::stringstream statements;

    statements << "record." << fieldId << "+=" << value << ";" << std::endl;
    cg.pipeline(pipeline).addInstruction(CMethod::Instruction(INSTRUCTION_PRINT, statements.str()));
  }
};

// subtract a value from a field or subtract one field from another
class Subtract : public Mapper {
public:
  Subtract(std::string fieldId, std::string value) : Mapper(fieldId, value) {}
  Subtract(std::string fieldId, std::string value, std::string outputField) : Mapper(fieldId, value, outputField) {}
  Subtract(std::string fieldId, long value) : Mapper(fieldId, value) {}
  Subtract(std::string fieldId, long value, std::string outputField) : Mapper(fieldId, value, outputField) {}
  Subtract(std::string fieldId, Field &value) : Mapper(fieldId, value) {}
  Subtract(std::string fieldId, Field &value, std::string outputField) : Mapper(fieldId, value, outputField) {}

  void consume(CodeGenerator &cg, Operator *parent) {}
};

// divides two fields or divides a field by a value
class Divide : public Mapper {
public:
  Divide(std::string fieldId, std::string value) : Mapper(fieldId, value) {}
  Divide(std::string fieldId, std::string value, std::string outputField) : Mapper(fieldId, value, outputField) {}
  Divide(std::string fieldId, long value) : Mapper(fieldId, value) {}
  Divide(std::string fieldId, long value, std::string outputField) : Mapper(fieldId, value, outputField) {}
  Divide(std::string fieldId, Field &value) : Mapper(fieldId, value) {}
  Divide(std::string fieldId, Field &value, std::string outputField) : Mapper(fieldId, value, outputField) {}

  void consume(CodeGenerator &cg, Operator *parent) {}
};

// concatinates two (string) fields
class Concat : public Mapper {
public:
  Concat(std::string fieldId, std::string value) : Mapper(fieldId, value) {}
  Concat(std::string fieldId, std::string value, std::string outputField) : Mapper(fieldId, value, outputField) {}
  Concat(std::string fieldId, Field &value) : Mapper(fieldId, value) {}
  Concat(std::string fieldId, Field &value, std::string outputField) : Mapper(fieldId, value, outputField) {}

  void consume(CodeGenerator &cg, Operator *parent) {}
};

#endif // API_MAPPER_H
