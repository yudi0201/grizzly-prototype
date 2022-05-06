#include <sstream>
#include <string>

#include "operator/WriteToMemOperator.h"

WriteToMemOperator::WriteToMemOperator(Operator *input) : input(input) {
  leftChild = NULL;
  rightChild = NULL;
  name = "WriteToMem";
  input->parent = this;
}

std::string WriteToMemOperator::to_string() { return "WriteToMem"; }

WriteToMemOperator::~WriteToMemOperator() { delete input; }

void WriteToMemOperator::consume(CodeGenerator &cg) {

  auto &record = cg.pipeline(pipeline).parameters[0];
  std::stringstream statements;
  cg.ctx(cg.currentPipeline()).outputSchema.print();

  statements << "buffers[thread_id].push_back(record);" << std::endl;

  std::stringstream intBufferStatement;
  intBufferStatement << "auto buffers = tbb::concurrent_unordered_map<int, std::vector<record" << cg.currentPipeline() << ">>();";
  cg.file.addStatement(intBufferStatement.str());

  statements << std::endl;
  cg.pipeline(pipeline).addInstruction(CMethod::Instruction(INSTRUCTION_PRINT, statements.str()));
}

void WriteToMemOperator::produce(CodeGenerator &cg) {
  pipeline = cg.currentPipeline();
  input->produce(cg);
}
