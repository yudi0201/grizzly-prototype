#ifndef OPERATOR_WRITE_MEM_OPERATOR_H
#define OPERATOR_WRITE_MEM_OPERATOR_H

#include "operator/Operator.h"

class WriteToMemOperator : public Operator {
public:
  WriteToMemOperator(Operator *input, bool print);
  ~WriteToMemOperator();
  void consume(CodeGenerator &cg);
  void produce(CodeGenerator &cg);
  std::string to_string();

private:
  Operator *input;
  bool print;
};

#endif // OPERATOR_WRITE_MEM_OPERATOR_H
