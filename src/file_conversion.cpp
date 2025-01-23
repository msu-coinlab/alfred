
#include "file_conversion.h"

#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

namespace file_conversion {
    // #1 Write out the data as a Parquet file
    void write_csv_file(const arrow::Table &table, const std::string csv_filename) {
        PARQUET_ASSIGN_OR_THROW(auto outstream, arrow::io::FileOutputStream::Open(csv_filename));
        PARQUET_THROW_NOT_OK(arrow::csv::WriteCSV(table, arrow::csv::WriteOptions::Defaults(), outstream.get()));
    }

    void parquet_to_csv(std::string parquet_filename, std::string csv_filename) {
        std::shared_ptr<arrow::io::ReadableFile> infile;
        PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(parquet_filename, arrow::default_memory_pool()));
        std::unique_ptr<parquet::arrow::FileReader> reader;
        PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
        std::shared_ptr<arrow::Table> table;
        PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
        write_csv_file(*table, csv_filename);
    }
}

