#include <string>
#include <fstream>
#include <vector>
#include "fmt/format.h"
#include "nlohmann/json.hpp"
#include <filesystem>
#include <sw/redis++/redis++.h>
#include <SimpleAmqpClient/SimpleAmqpClient.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include "config.h"
#include "utils.h"
#include "myawss3.h"
#include "myamqp.h"
#include "file_conversion.h"


unsigned long long my_time = 0;
using json = nlohmann::json;

namespace redis {
    const std::string REDIS_URL = fmt::format("tcp://{}:{}/{}", 
                                              config::get_env_var("REDIS_HOST"), 
                                              config::get_env_var("REDIS_PORT"), 
                                              config::get_env_var("REDIS_DB_OPT"));
    auto redis = sw::redis::Redis(REDIS_URL);
}

const std::string OPT4CAST_WAIT_MILLISECS_IN_CAST = config::get_env_var("OPT4CAST_WAIT_MILLISECS_IN_CAST");
std::string msu_cbpo_path = config::get_env_var("MSU_CBPO_PATH", "/opt/opt4cast");





std::tuple<json, json, json> create_jsons(std::string scenario_id, std::string exec_uuid) {

    std::string emo_data_str = *redis::redis.hget("emo_data", exec_uuid);
    std::vector<std::string> emo_data_list;
    utils::split_string(emo_data_str, '_', emo_data_list);

    std::string scenario_name = emo_data_list[0];
    int atm_dep_data_set_id = std::stoi(emo_data_list[1]);
    int back_out_scenario_id = std::stoi(emo_data_list[2]);
    int base_condition_id = std::stoi(emo_data_list[3]);
    int base_load_id = std::stoi(emo_data_list[4]);
    int cost_profile_id = std::stoi(emo_data_list[5]);
    int climate_change_data_set_id = std::stoi(emo_data_list[6]);
    int geography_nids = std::stoi(emo_data_list[7]);
    int historical_crop_need_scenario_id = std::stoi(emo_data_list[8]);
    int point_source_data_set_id = std::stoi(emo_data_list[9]);
    int scenario_type_id = std::stoi(emo_data_list[10]);
    int soil_p_data_set_id = std::stoi(emo_data_list[11]);
    int source_data_revision_id = std::stoi(emo_data_list[12]);

    std::vector<int> geography_id_list;
    int geography_tmp;
    fmt::print("geography_nids: {}\n", geography_nids);
    for (int i(0); i < geography_nids; ++i) {
        fmt::print("{}\n", emo_data_list[13 + i]);
        
        geography_tmp = std::stoi(emo_data_list[13 + i]);
        geography_id_list.push_back(geography_tmp);
    }

    json scenario;
    int scenario_id_int = std::stoi(scenario_id);

    scenario["ScenarioId"] = scenario_id_int;
    scenario["ScenarioName"] = scenario_name;
    scenario["BaseConditionId"] = base_condition_id;
    scenario["BackOutScenarioId"] = back_out_scenario_id;
    scenario["ScenarioTypeId"] = scenario_type_id;
    scenario["CostProfileId"] = cost_profile_id;
    scenario["SoilPDataSetId"] = soil_p_data_set_id;
    scenario["HistoricalCropNeedScenarioId"] = historical_crop_need_scenario_id;
    scenario["PointSourceDataSetId"] = point_source_data_set_id;
    scenario["AtmDepDataSetId"] = atm_dep_data_set_id;
    scenario["ClimateChangeDataSetId"] = climate_change_data_set_id;
    scenario["BaseLoadId"] = base_load_id;
    scenario["SourceDataRevisionId"] = source_data_revision_id;


    json user_data = json::parse(
            R"({ "EventText": "", "FirstName": "", "LastName": "", "Notify": [""], "EventKey": "RunScenario", "NotifiedBy": "", "NotifyReason": "", "UserId": "00000000-0000-0000-0000-000000000000" })");
    json data_transfer_object = json::parse(
            R"({ "CopyAg": [], "CopyDc": [], "CopyDe": [], "CopyMd": [], "CopyNy": [], "CopyPenn": [], "CopyVa": [], "CopyWva": [], "CopyNat": [], "CopySept": [], "CopyDev": [], "CopyPol": [], "CopySingle": [], "NeedsReRun": false, "IsPublic": false, "IsOptimizationMode": true, "NeedsValidation": false, "StatusId": 0, "Name": null, "Source": 0, "SourceDataVersion": "", "NetworkPath": "", "LandBmpImportFiles": [], "AimalBmpImportFiles": [], "ManureBmpImportFiles": [], "LandPolicyBmpImportFiles": [] })");
    data_transfer_object["Id"] = scenario_id_int;
    data_transfer_object["FileId"] = exec_uuid;
    data_transfer_object["UserData"] = user_data;
    json core = json::parse(R"({ "ProcessorFactory": "ScenarioProcessorFactory", "ProcessorServiceName": "ScenarioRunProcessor"})");
    core["DataTransferObject"] = data_transfer_object;

    json geography;
    // even easier with structured bindings (c++17)
    for (auto &&geography_id: geography_id_list) {
        json geography_item;
        geography_item["ScenarioId"] = scenario_id_int;
        geography_item["GeographyId"] = geography_id;
        geography.emplace_back(geography_item);
    }


    redis::redis.hset(exec_uuid, "core", core.dump());
    redis::redis.hset(exec_uuid, "scenario", scenario.dump());
    redis::redis.hset(exec_uuid, "geography", geography.dump());

    return std::make_tuple(core, scenario, geography);
}


bool send_json_streams(std::string scenario_id,
                       std::string emo_uuid,
                       std::string exec_uuid,
                       bool del_files = true) {
    std::string core_str;
    std::string scenario_str;
    std::string geography_str;

    if (redis::redis.hexists(emo_uuid, "core") == false) {
        create_jsons(scenario_id, emo_uuid);
    }
    core_str = *redis::redis.hget(emo_uuid, "core");
    scenario_str = *redis::redis.hget(emo_uuid, "scenario");
    geography_str = *redis::redis.hget(emo_uuid, "geography");

    int scenario_id_int = std::stoi(scenario_id);
    json core = json::parse(core_str);
    json scenario = json::parse(scenario_str);
    json geography = json::parse(geography_str);
    core["DataTransferObject"]["Id"] = scenario_id_int;
    core["DataTransferObject"]["FileId"] = exec_uuid;
    scenario["ScenarioId"] = scenario_id_int;

    for (auto &&geography_tmp: geography) {
        geography_tmp["ScenarioId"] = scenario_id_int;
    }
    std::string scenario_path = fmt::format("data/scenarios/metadata/scenario/scenarioid={}/scenario.json", scenario_id);
    fmt::print("scenario_path: {}\n", scenario_path);
    std::string scenario_geography_path = fmt::format("data/scenarios/metadata/scenariogeography/scenarioid={}/scenariogeography.json", scenario_id);
    fmt::print("scenario_geography_path: {}\n", scenario_geography_path);
    //bmps
    std::string impbmpsubmittedland_path = fmt::format("data/scenarios/metadata/impbmpsubmittedland/scenarioid={}/impbmpsubmittedland.parquet", scenario_id);
    fmt::print("impbmpsubmittedland_path: {}\n", impbmpsubmittedland_path);
    std::string impbmpsubmittedanimal_path = fmt::format("data/scenarios/metadata/impbmpsubmittedanimal/scenarioid={}/impbmpsubmittedanimal.parquet", scenario_id);
    std::string impbmpsubmittedmanuretransport_path = fmt::format("data/scenarios/metadata/impbmpsubmittedmanuretransport/scenarioid={}/impbmpsubmittedmanuretransport.parquet", scenario_id);
    //output
    std::string reportloads_path = fmt::format("data/scenarios/modeloutput/reportloads/scenarioid={}/reportloads.parquet", scenario_id);
    //thrigger

    std::string core_path = fmt::format("lambdarequests/optimize/optimizeSce_{}.json", exec_uuid);
    fmt::print("core_path: {}\n", core_path);

    try {
        if (awss3::put_object_buffer("cast-optimization-dev", scenario_path, scenario.dump()) == false)
            return false;
        awss3::wait_for_file(scenario_path);


        if (awss3::put_object_buffer("cast-optimization-dev", scenario_geography_path, geography.dump()) == false)
            return false;

        awss3::wait_for_file(scenario_geography_path);

        //delete
        if (del_files == true) {
            awss3::delete_object("cast-optimization-dev", impbmpsubmittedland_path);
            awss3::delete_object("cast-optimization-dev", impbmpsubmittedanimal_path);
            awss3::delete_object("cast-optimization-dev", impbmpsubmittedmanuretransport_path);
        }
        awss3::delete_object("cast-optimization-dev", reportloads_path);
        int seconds = 2;
        std::this_thread::sleep_until(std::chrono::system_clock::now() + std::chrono::seconds(seconds));
        if (awss3::is_object("cast-optimization-dev", reportloads_path) == true)
            return false;


        if (awss3::put_object_buffer("cast-optimization-dev", core_path, core.dump()) == true)
            return true;

    }
    catch (const std::exception &error) {
        auto cerror = fmt::format("S3_ERROR Sending files: {}", error.what());
        std::cout << cerror<<"\n"; 
    }

    return false;
}




bool send_files(std::string scenario_id, std::string emo_uuid, std::string exec_uuid) {
    auto land_path = fmt::format("data/scenarios/metadata/impbmpsubmittedland/scenarioid={}/impbmpsubmittedland.parquet", scenario_id);
    auto land_filename = fmt::format("{}/output/nsga3/{}/{}_impbmpsubmittedland.parquet", msu_cbpo_path, emo_uuid, exec_uuid);
    auto animal_path = fmt::format("data/scenarios/metadata/impbmpsubmittedanimal/scenarioid={}/impbmpsubmittedanimal.parquet", scenario_id);
    auto animal_filename = fmt::format("{}/output/nsga3/{}/{}_impbmpsubmittedanimal.parquet", msu_cbpo_path, emo_uuid, exec_uuid);
    auto manuretransport_path = fmt::format("data/scenarios/metadata/impbmpsubmittedmanuretransport/scenarioid={}/impbmpsubmittedmanuretransport.parquet", scenario_id);
    auto manuretransport_filename = fmt::format("{}/output/nsga3/{}/{}_impbmpsubmittedmanuretransport.parquet", msu_cbpo_path, emo_uuid, exec_uuid);
    auto file_sent = false;

    awss3::delete_object("cast-optimization-dev", land_path);
    awss3::delete_object("cast-optimization-dev", animal_path);
    awss3::delete_object("cast-optimization-dev", manuretransport_path);

    if (std::filesystem::exists(land_filename) &&
        awss3::put_object("cast-optimization-dev", land_path, land_filename)) {
        file_sent = true;
        fmt::print("land file sent: scenario_id: {}, exec_id: {}\n", scenario_id, exec_uuid);
    }

    if (std::filesystem::exists(animal_filename) &&
        awss3::put_object("cast-optimization-dev", animal_path, animal_filename)) {
        file_sent = true;
    }

    if (std::filesystem::exists(manuretransport_filename) &&
        awss3::put_object("cast-optimization-dev", manuretransport_path, manuretransport_filename)) {
        file_sent = true;
    }

    return file_sent;
}



std::vector<double> read_loads(std::string loads_filename) {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(loads_filename, arrow::default_memory_pool()));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::ChunkedArray> array;
    arrow::Datum sum;

    std::vector<double> loads;

    for (int col(7); col < 16; ++col) {
        PARQUET_THROW_NOT_OK(reader->ReadColumn(col, &array));
        PARQUET_ASSIGN_OR_THROW(sum, arrow::compute::Sum(array));
        double val = (std::dynamic_pointer_cast<arrow::DoubleScalar>(sum.scalar()))->value;
        loads.emplace_back(val);
    }
    return loads;
}

bool download_parquet_file(std::string base_url, std::string path, std::string base_filename, std::string prefix_filename, std::string scenario_id) {
    std::string url = fmt::format("data/scenarios/{}/{}/scenarioid={}/{}.parquet", base_url, base_filename, scenario_id, base_filename);

    if (std::filesystem::exists(path) == false) {
        if (std::filesystem::create_directories(path) == false) {
            auto clog = fmt::format("[CREATE_DIRECTORY] {} ", path);
        }
    }
    std::string filename;
    filename = fmt::format("{}/{}{}.parquet", path, prefix_filename, base_filename);

    auto filename_csv = fmt::format("{}/{}{}.csv", path, prefix_filename, base_filename);

    if (awss3::is_object("cast-optimization-dev", url) == false)
        return false;

    awss3::wait_to_download_file(url, filename, 3, 1);

    file_conversion::parquet_to_csv(filename, filename_csv);
    return true;
}

bool get_files(std::string scenario_id, std::string emo_uuid, std::string exec_uuid) {
    bool ret = false;;

    if (emo_uuid == exec_uuid) {
        std::string dir_path = fmt::format("{}/output/nsga3/{}/config", msu_cbpo_path, emo_uuid);
        std::string base_filename = "reportloads";
        std::string prefix_filename = "";
        ret = download_parquet_file("modeloutput", dir_path, base_filename, prefix_filename, scenario_id);
        base_filename = "animalfractioninlrseg";
        download_parquet_file("modeloutput", dir_path, base_filename, prefix_filename, scenario_id);
        base_filename = "bmpcreditedland";
        download_parquet_file("modeloutput", dir_path, base_filename, prefix_filename, scenario_id);
        base_filename = "manurenutrientsconfinement";
        download_parquet_file("modeloutput", dir_path, base_filename, prefix_filename, scenario_id);
        base_filename = "manurenutrientsdirectdeposit";
        download_parquet_file("modeloutput", dir_path, base_filename, prefix_filename, scenario_id);
        base_filename = "septicloads";
        download_parquet_file("modeloutput", dir_path, base_filename, prefix_filename, scenario_id);
    } else {
        std::string dir_path = fmt::format("{}/output/nsga3/{}", msu_cbpo_path, emo_uuid);
        std::string base_filename = "reportloads";
        std::string prefix_filename = fmt::format("{}_", exec_uuid);
        ret = download_parquet_file("modeloutput", dir_path, base_filename, prefix_filename, scenario_id);
    }
    return ret;
}



bool execute_20(std::string scenario_id, std::string emo_uuid, std::string exec_uuid) {
    bool got_files = false;
    got_files = get_files(scenario_id, emo_uuid, exec_uuid);
    if (got_files == true) {
        std::string loads_filename;
        if (emo_uuid == exec_uuid) {
            loads_filename = fmt::format("{}/output/nsga3/{}/config/reportloads.parquet", msu_cbpo_path, emo_uuid);
            std::string local_report_loads_csv = fmt::format("{}/output/nsga3/{}/config/reportloads.csv", msu_cbpo_path, emo_uuid);
            auto input_path = fmt::format("{}/input", msu_cbpo_path);
            if (std::filesystem::exists(input_path) == false) {
                auto ret = std::filesystem::create_directories(input_path);
            }
            std::string local_report_loads_csv2 = fmt::format("{}/input/{}_reportloads.csv", msu_cbpo_path, emo_uuid);
            std::filesystem::copy(local_report_loads_csv, local_report_loads_csv2);
        } else {
            loads_filename = fmt::format("{}/output/nsga3/{}/{}_reportloads.parquet", msu_cbpo_path, emo_uuid, exec_uuid);
        }
        auto loads = read_loads(loads_filename);
        redis::redis.hset("executed_results", exec_uuid, fmt::format("{:.2f}_{:.2f}_{:.2f}", loads[0], loads[1], loads[2]));


    } else {
        auto clog =  "Error in retrieving files: ";
        std::cout<<clog<<"\n";

        return false;
    }
    return got_files;
}

void retrieve_exec() {

    std::vector<std::string> to_retrieve_list;
    redis::redis.lrange("retrieving_queue", 0, -1, std::back_inserter(to_retrieve_list));
    int n_to_retrieve = to_retrieve_list.size();
    redis::redis.ltrim("retrieving_queue", n_to_retrieve, -1);

    if (n_to_retrieve > 0) {
        std::cout << "Retrieving: " << n_to_retrieve << std::endl;
        if (n_to_retrieve == 1) {
            std::string exec_str = *redis::redis.hget("exec_to_retrieve", to_retrieve_list[0]);
            std::vector<std::string> tmp_list;
            utils::split_string(exec_str, '_', tmp_list);
            fmt::print("Retrieving: {}, {}\n", to_retrieve_list[0], tmp_list[1]); 
        }
    }

    for (int index(0); index < n_to_retrieve; index++) {
        std::string exec_uuid = to_retrieve_list[index];
        if (redis::redis.hexists("started_time", exec_uuid) == false) {
            redis::redis.hset("started_time", exec_uuid, fmt::format("{}", utils::get_time()));
        }
        long added_time_l = 0;
        std::string exec_str = *redis::redis.hget("exec_to_retrieve", exec_uuid);
        std::vector<std::string> tmp_list;
        utils::split_string(exec_str, '_', tmp_list);

        std::string emo_uuid = tmp_list[0];
        std::string scenario_id = tmp_list[1];

        bool flag = false;

        if (awss3::is_scenario_finished(scenario_id) == true) {
            if (execute_20(scenario_id, emo_uuid, exec_uuid) == true) {
                auto cinfo = fmt::format("[Retrieved] EMO_UUID={}  EXEC_UUID={} Scenario ID={}", emo_uuid, exec_uuid, scenario_id);

                amqp::send_message(emo_uuid, exec_uuid);
                fmt::print("Retrieved: {}, {}\n", exec_uuid, scenario_id);
                redis::redis.rpush("scenario_ids", scenario_id);
                redis::redis.hdel("exec_to_retrieve", exec_uuid);
                redis::redis.hdel("started_time", exec_uuid);
                continue;
            }
        }

        unsigned long long now_millisec = utils::get_time();
        unsigned long long started_time = std::stol(*redis::redis.hget("started_time", exec_uuid));
        unsigned long long waiting_time = 100000;
        try {
            waiting_time = std::stol(OPT4CAST_WAIT_MILLISECS_IN_CAST);
        }
        catch (const std::exception &error) {
            std::cout << "Error on retrieve_exec\n" << error.what() << "\n";
            auto cerror =  fmt::format("When trying using stoi for (OPT4CAST_WAIT_MILLISECS_IN_CAST): {} ", error.what());
        }
        std::string core_path = fmt::format("lambdarequests/optimize/optimizeSce_{}.json", exec_uuid);

        if (awss3::is_object("cast-optimization-dev", core_path) == false || 
                now_millisec - started_time > waiting_time) 
        {
            double fake_load = 9999999999999.99; //DBL_MAX; would it work?
            std::cout << fmt::format("{} - {} = {}: Waiting time: {}\n", now_millisec, started_time, now_millisec - started_time, waiting_time);

            redis::redis.hset("executed_results", exec_uuid, fmt::format("{:.2f}_{:.2f}_{:.2f}", fake_load, fake_load, fake_load));
            amqp::send_message(emo_uuid, exec_uuid);
            redis::redis.rpush("scenario_ids", scenario_id);
            redis::redis.hdel("exec_to_retrieve", exec_uuid);
            redis::redis.hdel("started_time", exec_uuid);
        } else {
            redis::redis.rpush("retrieving_queue", exec_uuid);
        }
    }

}



bool emo_to_initialize(std::string emo_uuid) {

    auto scenario_id = *redis::redis.hget("emo_to_initialize", emo_uuid);
    auto [core, scenario, geography] = create_jsons(scenario_id, emo_uuid);
    auto core_str = *redis::redis.hget(emo_uuid, "core");
    auto scenario_str = *redis::redis.hget(emo_uuid, "scenario");
    auto geography_str = *redis::redis.hget(emo_uuid, "geography");

    auto exec_uuid = emo_uuid;


    if (send_json_streams(scenario_id, emo_uuid, exec_uuid) == true) {
        std::cout << fmt::format("emo_to_initialize emo_uuid: {} scenario_id: {}\n", emo_uuid, scenario_id);

        auto cinfo =  fmt::format("[Initialzing] EMOO_UUID: {} Scenario ID: {}", emo_uuid, scenario_id);
        redis::redis.hset("exec_to_retrieve", exec_uuid, fmt::format("{}_{}", emo_uuid, scenario_id));
        redis::redis.rpush("retrieving_queue", exec_uuid);

        redis::redis.hset("started_time", exec_uuid, fmt::format("{}", utils::get_time()));
        cinfo =  fmt::format("[Retrieving QUEUE] EMOO_UUID: {} Scenario ID: {}", emo_uuid, scenario_id);
        if (redis::redis.hdel("emo_to_initialize", emo_uuid)) {

            cinfo =  fmt::format("[EMO Initialized and removed from queue] EMOO_UUID: {} Scenario ID: {}", emo_uuid, scenario_id);
        } else {
            auto cerror =  fmt::format("[[DELETE EMO FROM INITALIZED FAILED] EMOO_UUID: {} Scenario ID: {}", emo_uuid, scenario_id);
        }
    } else {
        redis::redis.rpush("emo_failed_to_initialize", emo_uuid);
        std::clog << "it did not sent \n";
        auto cerror = fmt::format("[INITIALIZZATION_FAILED] EMOO_UUID: {} Scenario ID: {}", emo_uuid, scenario_id);
    }
    return true;
}

bool solution_to_execute(std::string exec_uuid) {
    std::string exec_str = *redis::redis.hget("solution_to_execute_dict", exec_uuid);
    if (exec_str.size() <= 0) {
        std::cout << "hay pedo\n";
        exit(-1);
    }
    std::vector<std::string> exec_list;
    utils::split_string(exec_str, '_', exec_list);

    auto emo_uuid = exec_list[0];
    auto scenario_id = exec_list[1];

    if (redis::redis.hexists(emo_uuid, "core") == false) {
        create_jsons(scenario_id, emo_uuid);
    }

    auto core_str = *redis::redis.hget(emo_uuid, "core");
    auto scenario_str = *redis::redis.hget(emo_uuid, "scenario");
    auto geography_str = *redis::redis.hget(emo_uuid, "geography");

    bool flag = true;
    if (send_files(scenario_id, emo_uuid, exec_uuid) == true) {
        if (send_json_streams(scenario_id, emo_uuid, exec_uuid, false) == true) {
            auto cinfo =  fmt::format("[Executing] EXEC_UUID: {} EMOO_UUID: {} Scenario ID: {} ", exec_uuid, emo_uuid, scenario_id);
            redis::redis.hset("exec_to_retrieve", exec_uuid, fmt::format("{}_{}", emo_uuid, scenario_id));
            redis::redis.rpush("retrieving_queue", exec_uuid);
            redis::redis.hset("started_time", exec_uuid, fmt::format("{}", utils::get_time()));
            cinfo =  fmt::format("[Retrieving QUEUE] EXEC_UUID: {} EMOO_UUID: {} Scenario ID: {}", exec_uuid, emo_uuid, scenario_id);
            redis::redis.hdel("solution_to_execute_dict", exec_uuid);
            if (redis::redis.hexists("solution_to_execute_dict", exec_uuid)) {
                auto cerror = fmt::format("[solution was not successfully deleted it] EMOO_UUID: {} Scenario ID: {}", emo_uuid, scenario_id);
                std::cout<<cerror<<"\n";
            }
        } else {
            auto cerror =  fmt::format("[SCENARIO_SUBMISSION] EMOO_UUID: {} EXEC_UUID: {} Scenario ID: {}\n", emo_uuid, exec_uuid, scenario_id);
            flag = false;
        }
    } else {
            auto cerror =  fmt::format("[SEND_TO_EXECUTION_FAILED] EMOO_UUID: {} EXEC_UUID: {} Scenario ID: {}\n", emo_uuid, exec_uuid, scenario_id);
            std::clog << cerror;
            std::cout<<cerror<<"\n";
            flag = false;
    }
    if (flag == false) {
        std::cout<<"Returning in Solution to execute==================="<<"\n";


        double fake_load = 9999999999999.99; //DBL_MAX; would it work?

        redis::redis.hset("executed_results", exec_uuid, fmt::format("{:.2f}_{:.2f}_{:.2f}", fake_load, fake_load, fake_load));
        amqp::send_message(emo_uuid, exec_uuid);
        redis::redis.hdel("started_time", exec_uuid);
        if (redis::redis.hexists("exec_to_retrieve", exec_uuid)) {
            redis::redis.hdel("exec_to_retrieve", exec_uuid);
        }
        redis::redis.rpush("scenario_ids", scenario_id);
    }

    return flag;
}

void send() {

    AmqpClient::Channel::OpenOpts opts;
    fmt::print("AMQP_HOST: {}\n", amqp::AMQP_HOST);
    opts.host = amqp::AMQP_HOST;
    fmt::print("AMQP_PORT: {}\n", amqp::AMQP_PORT);
    opts.port = std::stoi(amqp::AMQP_PORT);
    fmt::print("AMQP_USERNAME: {}\n", amqp::AMQP_USERNAME);
    std::string username = amqp::AMQP_USERNAME;
    std::string password = amqp::AMQP_PASSWORD;
    opts.vhost = "/";
    opts.frame_max = 131072;
    opts.auth = AmqpClient::Channel::OpenOpts::BasicAuth(username, password);
    //try {
        auto channel = AmqpClient::Channel::Open(opts);

        auto passive = false; //meaning you want the server to create the exchange if it does not already exist.
        auto durable = true; //meaning the exchange will survive a broker restart
        auto auto_delete = false; //meaning the queue will not be deleted once the channel is closed
        channel->DeclareExchange(amqp::EXCHANGE_NAME, AmqpClient::Channel::EXCHANGE_TYPE_DIRECT, passive, durable, auto_delete);

        auto generate_queue_name ="";
        auto exclusive = false; //meaning the queue can be accessed in other channels
        auto queue_name = channel->DeclareQueue(generate_queue_name, passive, durable, exclusive, auto_delete);


        auto cinfo =  fmt::format("Queue with name {} has been declared.\n", queue_name);
        channel->BindQueue(queue_name, amqp::EXCHANGE_NAME, "opt4cast_initialization");
        channel->BindQueue(queue_name, amqp::EXCHANGE_NAME, "opt4cast_execution");

        channel->BindQueue(queue_name, amqp::EXCHANGE_NAME, "opt4cast_mathmodel_execution");
        channel->BindQueue(queue_name, amqp::EXCHANGE_NAME, "opt4cast_begin_generation");
        channel->BindQueue(queue_name, amqp::EXCHANGE_NAME, "opt4cast_end_generation");
        channel->BindQueue(queue_name, amqp::EXCHANGE_NAME, "opt4cast_begin_emo");
        channel->BindQueue(queue_name, amqp::EXCHANGE_NAME, "opt4cast_end_emo");
        channel->BindQueue(queue_name, amqp::EXCHANGE_NAME, "opt4cast_get_var_and_cnstr");

        std::cout << " [*] Waiting for executions. To exit press CTRL+C\n";

        auto no_local = false; 
        auto no_ack = true; 
        auto message_prefetch_count = 1;
        auto consumer_tag = channel->BasicConsume(queue_name, generate_queue_name, no_local, no_ack, exclusive, message_prefetch_count);


        cinfo =  fmt::format("Consumer tag: {}\n", consumer_tag);
        while (true) {
            auto envelope = channel->BasicConsumeMessage(consumer_tag);
            auto message_payload = envelope->Message()->Body();
            auto routing_key = envelope->RoutingKey();

            if (routing_key == std::string("opt4cast_initialization")) {
                auto cinfo =  fmt::format("Initializing scenario: {}", message_payload);
                std::cout << "Initializing scenario: " << message_payload << "\n";
                emo_to_initialize(message_payload);
            } else if (routing_key == std::string("opt4cast_execution")) {

                auto cinfo =  fmt::format("Sending scenario to execution queue", message_payload);
                std::cout << "sending execution scenario: " << message_payload << "\n";


                int concurrent_evaluation = 10;
                int seconds = 1;
                std::vector<std::string> to_retrieve_list;
                while (to_retrieve_list.size() >= concurrent_evaluation) {
                    std::this_thread::sleep_until(std::chrono::system_clock::now() + std::chrono::seconds(seconds));
                    to_retrieve_list.clear();
                    redis::redis.lrange("retrieving_queue", 0, -1, std::back_inserter(to_retrieve_list));
                }
                solution_to_execute(message_payload);

            }
        }

}

bool retrieve() {
        while (true) {
            retrieve_exec();



            std::vector<std::string> init_failed_list;

            redis::redis.lrange("emo_failed_to_initialize", 0, -1, std::back_inserter(init_failed_list));
            for (auto emo_uuid: init_failed_list) {
                std::cout << "emo_to_initilize\n";
                emo_to_initialize(emo_uuid);
            }


            std::vector<std::string> failed_exec_uuids;
            redis::redis.lrange("solution_failed_to_execute_dict", 0, -1, std::back_inserter(failed_exec_uuids));
            redis::redis.del({"solution_failed_to_execute_dict"});
            for (auto exec_uuid: failed_exec_uuids) {
                std::cout << "solution_to_execute\n";
                solution_to_execute(exec_uuid);
            }


            int seconds = 1;
            std::this_thread::sleep_until(std::chrono::system_clock::now() + std::chrono::seconds(seconds));
        }
    return false;
}


int main(int argc, char const *argv[]) {
    my_time = utils::get_time();
    std::string env_var = "MSU_CBPO_PATH";
    msu_cbpo_path = config::get_env_var(env_var);
    std::cout << "msu_cbpo_path" << msu_cbpo_path << std::endl;

    std::string options[] = {"send", "retrieve", "log", "initialize_and_retrieve_data"};

    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " [send | retrieve | log]\n";
        return -1;
    }

    auto current_option = argv[1];

    Aws::SDKOptions aws_options;
    aws_options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Error;
    Aws::InitAPI(aws_options);
    {

        if (find(cbegin(options), cend(options), current_option) == cend(options)) {
            std::cerr << "Invalid option: " << current_option << ", please use [send | retrieve]\n";
            return -2;
        }

        if (current_option == std::string("send")) {
            auto cinfo =  "Starting send scenario deamon";
            while (!amqp::check()) {
                int seconds = 10;
                std::cout << fmt::format("Sleeping for {} seconds\n", seconds);
                sleep(seconds);
            }
            send();
        } else if (current_option == std::string("retrieve")) {
            auto cinfo =  "Starting retrieve scenario deamon";
            while (!amqp::check()) {
                int seconds = 10;
                std::cout << fmt::format("Sleeping for {} seconds\n", seconds);
                sleep(seconds);
            }
            retrieve();
        }/* else if (current_option == std::string("initalize_and_retrieve_data")) {
            std::cout << "initialize_and_retrieve_data" << std::endl;
            auto cinfo =  "Initialize and retrieve Data";
        }*/
    }

    ShutdownAPI(aws_options);
    return 0;
}

