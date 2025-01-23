#include <iostream>
#include <string>

#include <SimpleAmqpClient/SimpleAmqpClient.h>
#include <fmt/format.h>
#include "config.h"

namespace amqp {
    std::string EXCHANGE_NAME = "opt4cast_exchange";
    std::string AMQP_HOST = config::get_env_var("AMQP_HOST");
    std::string AMQP_USERNAME = config::get_env_var("AMQP_USERNAME");
    std::string AMQP_PASSWORD = config::get_env_var("AMQP_PASSWORD");
    std::string AMQP_PORT = config::get_env_var("AMQP_PORT");

    bool send_message(std::string routing_name, std::string msg) {
        bool return_val = false;
    
        AmqpClient::Channel::OpenOpts opts;
        opts.host = amqp::AMQP_HOST.c_str();
        opts.port = std::stoi(amqp::AMQP_PORT);
        opts.vhost = "/";
        opts.frame_max = 131072;
        opts.auth = AmqpClient::Channel::OpenOpts::BasicAuth(amqp::AMQP_USERNAME, amqp::AMQP_PASSWORD);
        try {
            auto channel = AmqpClient::Channel::Open(opts);
            channel->DeclareExchange(amqp::EXCHANGE_NAME, AmqpClient::Channel::EXCHANGE_TYPE_DIRECT, false, true, false);
            // Enable publisher confirmations for the channel.
            auto message = AmqpClient::BasicMessage::Create(msg);
            channel->BasicPublish(amqp::EXCHANGE_NAME, routing_name, message, false, false);
            return_val = true;
        }
        catch (const std::exception &error) {
            auto cerror = fmt::format("error {} ", error.what());
            std::cout << "Error in send Message to RabbitMQ " << error.what() << "\n";
        }
    
        return return_val;
    }
    bool check() {
        AmqpClient::Channel::OpenOpts opts;
        opts.host = amqp::AMQP_HOST;
        opts.port = std::stoi(amqp::AMQP_PORT);
        std::string username = amqp::AMQP_USERNAME;
        std::string password = amqp::AMQP_PASSWORD;
        opts.vhost = "/";
        opts.frame_max = 131072;
        opts.auth = AmqpClient::Channel::OpenOpts::BasicAuth(username, password);
        auto ret_value = false;
        try {
            auto channel = AmqpClient::Channel::Open(opts);
            ret_value = true;
        }
        catch (const std::exception &error) {
            auto cerror =  fmt::format("Error on sending scenarios: ", error.what());
        }
        return ret_value;
    }
    
}

