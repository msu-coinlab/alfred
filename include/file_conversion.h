#ifndef FILE_CONVERSION_H
#define FILE_CONVERSION_H
#include <string>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/csv/api.h>
#include <arrow/csv/writer.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/compute/api_aggregate.h>


namespace file_conversion {
    // #1 Write out the data as a Parquet file
    void write_csv_file(const arrow::Table &table, const std::string csv_filename);

    void parquet_to_csv(std::string parquet_filename, std::string csv_filename);
}
#endif
