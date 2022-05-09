#include "api/Config.h"
#include "api/Field.h"
#include "api/Query.h"
#include "api/Schema.h"
#include "code_generation/CodeGenerator.h"
#include <string>

int main(int argc, const char *argv[]) {

  if(argc != 5){
    std::cout << "Please provide the right argument. "
                 "1. parallelism, "
                 "2. buffer size in tuple, "
                 "3. execution duration for the query, "
                 "4. path to input data" << std::endl;
    return -1;
  }

  auto parallelism = std::stoi(argv[1]);
  auto bufferSize = std::stoi(argv[2]);
  auto experimentDuration = std::stoi(argv[3]);
  auto path = argv[4];

  Config config = Config::create()
                      // configures the number of worker threads
                      .withParallelism(parallelism)
                      // the number of records per input buffer ->
                      // this has to correspond to the number of records in the input file
                      .withBufferSize(bufferSize)
                      // the number of records processed per pipeline invocation ->
                      // if this is equal to the buffer size the pipeline will always process the whole input buffer.
                      .withRunLength(bufferSize)
                      // configures how many seconds the benchmark will be executed.
                      .withBenchmarkRunDuration(experimentDuration)
                      // configures the time in ms the jit waits to switch to the next compilation stage.
                      .withCompilationDelay(4000)
                      // enables filter predicate optimizations
                      .withFilterOpt(true)
                      // enables key distribution optimizations
                      .withDistributionOpt(true);

  Schema schema = Schema::create()
                      .addFixSizeField("start_time", DataType::Long, Stream)
                      .addFixSizeField("end_time", DataType::Long, Stream)
                      .addFixSizeField("payload", DataType::Double, Stream);

  Query::generate(config, schema, path)
//	.map(Add("payload", "3"))
      //.filter(new GreaterEqual("payload", 0))
      .window(TumblingProcessingTimeWindow(Time::seconds(1)))
      .aggregate(Avg("payload"))
      // prints output stream to console
      .print()
      .execute();

  return 0;
}
