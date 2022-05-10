#include "api/Config.h"
#include "api/Field.h"
#include "api/Query.h"
#include "api/Schema.h"
#include "code_generation/CodeGenerator.h"
#include <string>

int main(int argc, const char *argv[])
{
  auto testcase = (argc > 1) ? argv[1] : "select";
  auto parallelism = (argc > 2) ? std::stoi(argv[2]) : 1;
  auto bufferSize = (argc > 3) ? std::stoi(argv[3]) : 10000000;
  auto experimentDuration = (argc > 4) ? std::stoi(argv[4]) : 10;
  auto path = (argc > 5) ? argv[5] : "../data-generator/test_data.bin";
  auto period = 1;

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

  if (testcase == "select") {
    Query::generate(config, schema, path)
        .map(Add("payload", 3))
        .toOutputBuffer()
        .run();
  } else if (testcase == "where") {
    Query::generate(config, schema, path)
        .filter(new GreaterEqual("payload", 0))
        .toOutputBuffer()
        .run();
  } else if (testcase == "aggregate") {
    Query::generate(config, schema, path)
        .window(TumblingProcessingTimeWindow(Time::seconds(1)))
        .aggregate(Avg("payload"))
        .toOutputBuffer()
        .run();
  } else if (testcase == "alterdur") {
    Query::generate(config, schema, path)
        .map(Add("start_time", 10 * period, "end_time"))
        .toOutputBuffer()
        .run();
  } else {
    throw std::runtime_error("Invalid testcase");
  }

  return 0;
}
