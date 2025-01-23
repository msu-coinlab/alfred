
#include <string>
#include <fstream>
#include <sys/stat.h>
#include "fmt/format.h"
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <thread>
#include <chrono>

#include "utils.h"
#include "myawss3.h"


namespace awss3 {
    bool put_object(const Aws::String &bucket_name,
                    const Aws::String &object_name,
                    const Aws::String &local_object_name,
                    const Aws::String &region) {
        struct stat buffer;
        bool ret;

        if (stat(local_object_name.c_str(), &buffer) == -1) {
            std::string cerror = fmt::format("[S3] [LOCAL_FILE] [DOESNT_EXIST] [{}]", local_object_name);

            return false;
        }

        Aws::Client::ClientConfiguration config;

        if (!region.empty()) {
            config.region = region;
        }

        Aws::S3::S3Client s3_client(config);
        
        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(bucket_name);
        request.SetKey(object_name);

        std::shared_ptr<Aws::IOStream> input_data = Aws::MakeShared<Aws::FStream>(
                "sampleallocationtag", local_object_name.c_str(),
                std::ios_base::in | std::ios_base::binary);

        request.SetBody(input_data);

        Aws::S3::Model::PutObjectOutcome outcome = s3_client.PutObject(request);

        if (outcome.IsSuccess()) {
            ret = true;
        } else {

            /*
            unsigned long long my_new_time = (utils::get_time() - my_time) / 1000;
            std::string cerror = fmt::format("[S3] [PUT_OBJECT] {} {} {} {} {}", outcome.GetError().GetMessage(), my_new_time, bucket_name, object_name, local_object_name);
            */
            ret = false;
        }
        return ret;
    }

    bool put_object_buffer(const Aws::String &bucket_name,
                           const Aws::String &object_name,
                           const std::string &object_content,
                           const Aws::String &region) {

        bool ret = true;
        Aws::Client::ClientConfiguration config;

        if (!region.empty()) {
            config.region = region;
        }

        Aws::S3::S3Client s3_client(config);

        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(bucket_name);
        request.SetKey(object_name);

        const std::shared_ptr<Aws::IOStream> input_data =
                Aws::MakeShared<Aws::StringStream>("");
        *input_data << object_content.c_str();

        request.SetBody(input_data);

        Aws::S3::Model::PutObjectOutcome outcome = s3_client.PutObject(request);

        if (!outcome.IsSuccess()) {
            std::string cerror= fmt::format("[S3] [PUT_OBJECT] {}", outcome.GetError().GetMessage());

            ret = false;;
        } else {
            ret = true;
        }

        return ret;
    }

    bool get_object(const Aws::String &bucket_name,
                    const Aws::String &object_name,
                    const std::string &local_object_name,
                    const Aws::String region) {

        bool ret = true;
        Aws::Client::ClientConfiguration config;
        if (!region.empty()) {
            config.region = region;
        }

        Aws::S3::S3Client s3_client(config);

        Aws::S3::Model::GetObjectRequest object_request;

        object_request.SetBucket(bucket_name);
        object_request.SetKey(object_name);
        object_request.SetResponseStreamFactory([=]() {
            return Aws::New<Aws::FStream>("s3download", local_object_name,
                                          std::ios_base::out | std::ios_base::binary);
        });
        Aws::S3::Model::GetObjectOutcome get_object_outcome =
                s3_client.GetObject(object_request);

        if (get_object_outcome.IsSuccess()) {
            ret = true;
        } else {
            auto err = get_object_outcome.GetError();

            /*int my_new_time = (utils::get_time() - my_time) / 1000;
            std::string cerror = fmt::format("[S3] [GET_OBJECT] {} {} MS:{} {} {} {}", err.GetExceptionName(), err.GetMessage(), my_new_time, bucket_name, object_name, local_object_name);
            */

            ret = false;
        }
        return ret;
    }

    bool is_object(const Aws::String &bucket_name,
                   const Aws::String &object_name,
                   const Aws::String region) {
        bool ret = false;
        Aws::Client::ClientConfiguration config;
        if (!region.empty()) {
            config.region = region;
        }

        Aws::S3::S3Client s3_client(config);

        Aws::S3::Model::GetObjectRequest object_request;

        object_request.SetBucket(bucket_name);
        object_request.SetKey(object_name);

        Aws::S3::Model::GetObjectOutcome get_object_outcome =
                s3_client.GetObject(object_request);

        if (get_object_outcome.IsSuccess()) {
            ret = true;
        }
        return ret;
    }

    bool delete_object(const Aws::String &from_bucket,
                       const Aws::String &object_key) {
        bool ret = true;
        Aws::String region = "us-east-1";

        Aws::Client::ClientConfiguration clientconfig;
        if (!region.empty())
            clientconfig.region = region;

        Aws::S3::S3Client client(clientconfig);
        Aws::S3::Model::DeleteObjectRequest request;

        request.WithKey(object_key).WithBucket(from_bucket);

        Aws::S3::Model::DeleteObjectOutcome outcome = client.DeleteObject(request);

        if (!outcome.IsSuccess()) {
            ret = false;
        } else {
            ret = true;
        }
        return ret;
    }


    bool is_file_present(std::string filename) {
        try {
            if (awss3::is_object("cast-optimization-dev", filename) == true) {
                return true;
            }
    
        } catch (int e) {
            std::cout << "error: " << e << std::endl;
        }
        return false;
    }
    
    bool wait_for_file(std::string filename) {
        bool ret = false;
        int counter = 0;
        while (true) {
            if (is_file_present(filename) == false) {
                std::cout << "File has not finished: " << std::endl;
                if (counter > 100)
                    break;
    
                int seconds = 1;
                //sleep(seconds);
                //std::this_thread::sleep_until(std::chrono::system_clock::now() + std::chrono::seconds(seconds));
                std::this_thread::sleep_for(std::chrono::seconds(seconds));
                counter++;
            } else {
                ret = true;
                break;
            }
        }
    
        return ret;
    }

    bool wait_to_download_file(std::string url, std::string filename, int ntries, int wait_nsecs) {
        bool ans = false;
        while (true) {
            ans = awss3::get_object("cast-optimization-dev", url, filename);
            if (ans == true || ntries == 0) {
                break;
            } else {
                --ntries;
                //std::this_thread::sleep_for(std::chrono::seconds(wait_nsecs));
                std::this_thread::sleep_until(std::chrono::system_clock::now() + std::chrono::seconds(wait_nsecs));
            }
        }
        if (ans == false) {
            auto clog = fmt::format("We could not download file {} from {} after {} seconds",filename, url, wait_nsecs);
            return false;
        }
        return true;
    }

    
    bool is_scenario_finished(std::string scenario_id) {
        std::string reportloads_path = fmt::format("data/scenarios/modeloutput/reportloads/scenarioid={}/reportloads.parquet", scenario_id);
        try {
            if (awss3::is_object("cast-optimization-dev", reportloads_path) == true) {
                return true;
            }
    
        } catch (int e) {
            std::cout << "error: " << e << std::endl;
        }
        return false;
    }

    
    bool delete_modeloutput_files(std::string scenario_id) {
        std::string bucket_name = "cast-optimization-dev";
        std::string filename;
    
        //modeloutput
        filename = fmt::format("data/scenarios/modeloutput/animalfractioninlrseg/scenarioid={}/animalfractioninlrseg.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/modeloutput/bmpcreditedanimal/scenarioid={}/bmpcreditedanimal.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/modeloutput/bmpcreditedland/scenarioid={}/bmpcreditedland.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/modeloutput/bmpcreditedmanuretransport/scenarioid={}/bmpcreditedmanuretransport.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/modeloutput/cropapplicationmassmonthly/scenarioid={}/cropapplicationmassmonthly.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/modeloutput/cropcoverresults/scenarioid={}/cropcoverresults.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/modeloutput/croplanduse/scenarioid={}/croplanduse.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/modeloutput/cropnitrogenfixationresults/scenarioid={}/cropnitrogenfixationresults.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/modeloutput/cropnutrientsapplied/scenarioid={}/cropnutrientsapplied.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/modeloutput/landusepostbmp/scenarioid={}/landusepostbmp.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/modeloutput/loadseor/scenarioid={}/loadseor.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/modeloutput/loadseos/scenarioid={}/loadseos.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/modeloutput/loadseot/scenarioid={}/loadseot.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/modeloutput/loadssallbsperacre/scenarioid={}/loadssallbsperacre.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/modeloutput/manurenutrientsconfinement/scenarioid={}/manurenutrientsconfinement.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/modeloutput/manurenutrientsdirectdeposit/scenarioid={}/manurenutrientsdirectdeposit.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/modeloutput/manurenutrientstransported/scenarioid={}/manurenutrientstransported.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/modeloutput/reportloads/scenarioid={}/reportloads.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/modeloutput/septicloads/scenarioid={}/septicloads.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/modeloutput/septicsystemspostbmp/scenarioid={}/septicsystemspostbmp.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        return true;
    }
    
    bool delete_metadata_files(std::string scenario_id) {
        std::string bucket_name = "cast-optimization-dev";
        std::string filename;
        //metadata
        filename = fmt::format("data/scenarios/metadata/impbmpsubmittedanimal/scenarioid={}/impbmpsubmittedanimal.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/metadata/impbmpsubmittedland/scenarioid={}/impbmpsubmittedland.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/metadata/impbmpsubmittedmanuretransport/scenarioid={}/impbmpsubmittedmanuretransport.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/metadata/impbmpsubmittedrelatedmeasure/scenarioid={}/impbmpsubmittedrelatedmeasure.parquet", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/metadata/scenario/scenarioid={}/scenario.json", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/metadata/scenariogeography/scenarioid={}/scenariogeography.json", scenario_id);
        awss3::delete_object(bucket_name, filename);
        return true;
    }
    
    bool delete_indexes_files(std::string scenario_id) {
        std::string bucket_name = "cast-optimization-dev";
        std::string filename;
        //indexes
        filename = fmt::format("data/scenarios/indexes/scenarioid={}/landuse/BmpToLandUseIndexes.index", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/indexes/scenarioid={}/landuse/LandUseIndexes.index", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/indexes/scenarioid={}/lrsegfraction/BmpToLandRiverFraction.index", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/indexes/scenarioid={}/lrsegfraction/LandRiverFractionIndexes.index", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/indexes/scenarioid={}/septicuse/BmpToSepticUseIndexes.index", scenario_id);
        awss3::delete_object(bucket_name, filename);
        filename = fmt::format("data/scenarios/indexes/scenarioid={}/streamuse/BmpToStreamShoreUseIndexes.index", scenario_id);
        awss3::delete_object(bucket_name, filename);
        return true;
    }
    
    bool delete_files(std::string scenario_id) {
        delete_modeloutput_files(scenario_id);
        delete_indexes_files(scenario_id); 
        delete_metadata_files(scenario_id);
        return true;
    }

} // namespace awss3
