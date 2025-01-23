#ifndef MY_AMQP_H
#define MY_AMQP_H
#include <string>

namespace amqp {

    extern std::string EXCHANGE_NAME;
    extern std::string AMQP_HOST;
    extern std::string AMQP_USERNAME;
    extern std::string AMQP_PASSWORD;
    extern std::string AMQP_PORT;
    bool send_message(std::string routing_name, std::string msg);
    bool check();
}
#endif
