#include <sstream>
#include <string>

#include "operator/WriteToMemOperator.h"

WriteToMemOperator::WriteToMemOperator(Operator *input, bool print) : input(input), print(print) {
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

  statements << "auto idx = buf_sizes[thread_id]++;\n";
  statements << "buffers[thread_id][idx%BUF_SIZE] = record;" << std::endl;
  if (print) {
    statements << "std::cout << buffers[thread_id][idx%BUF_SIZE].to_string() << std::endl;" << std::endl;
  }

  std::stringstream intBufferStatement;
  intBufferStatement << "#include <unordered_map>\n";
  intBufferStatement << "auto buffers = std::unordered_map<int, std::vector<record" << cg.currentPipeline() << ">>();\n";
  intBufferStatement << "auto buf_sizes = std::vector<size_t>();\n";
  cg.file.addStatement(intBufferStatement.str());

  statements << std::endl;
  cg.pipeline(pipeline).addInstruction(CMethod::Instruction(INSTRUCTION_PRINT, statements.str()));
}

void WriteToMemOperator::produce(CodeGenerator &cg) {
  pipeline = cg.currentPipeline();
  input->produce(cg);
}
