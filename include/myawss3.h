#ifndef MYAWSS3_H
#define MYAWSS3_H

#include <aws/core/Aws.h>
#include <string.h>
#include <sys/stat.h>

namespace awss3 {
    bool put_object(const Aws::String &bucket_name,
                    const Aws::String &object_name,
                    const Aws::String &local_object_name,
                    const Aws::String &region = "us-east-1");

    bool put_object_buffer(const Aws::String &bucket_name,
                           const Aws::String &object_name,
                           const std::string &object_content,
                           const Aws::String &region = "us-east-1");

    bool get_object(const Aws::String &bucket_name,
                    const Aws::String &object_name,
                    const std::string &local_object_name,
                    const Aws::String region = "us-east-1");


    bool is_object(const Aws::String &bucket_name,
                   const Aws::String &object_name,
                   const Aws::String region = "us-east-1");

    bool delete_object(const Aws::String &from_bucket,
                       const Aws::String &object_key);
    bool is_file_present(std::string filename);
    
    bool wait_for_file(std::string filename);
    bool wait_to_download_file(std::string url, std::string filename, int ntries, int wait_nsecs);
    
    bool is_scenario_finished(std::string scenario_id);
    bool delete_modeloutput_files(std::string scenario_id);
    bool delete_metadata_files(std::string scenario_id);
    bool delete_indexes_files(std::string scenario_id);
    bool delete_files(std::string scenario_id);

} // namespace awss3
  //
#endif
