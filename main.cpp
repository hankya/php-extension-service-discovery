#include <phpcpp.h>
#include <ostream>
#include "zookeeper.hpp"
#include "process.hpp"

const char *Config_Servers_Key = "service-discovery.servers";
std::map<std::string, Php::Value> services;

Php::Value getService(Php::Parameters &params) {
    return services["hello"];
}

//class StateWatcher : public Watcher {
//public:
//    StateWatcher() { }
//
//private:
//    virtual void process(int type, int state, int64_t sessionId, const std::string &path);
//
//    bool reconnect;
//
//    void onConnected(int64_t id, const std::string &basic_string);
//
//    void onReconnecting(int64_t id, const std::string &basic_string);
//
//    void onExpired(int64_t id, const std::string &basic_string);
//};

/**
 *  tell the compiler that the get_module is a pure C function
 */
extern "C" {
/**
 *  Function that is called by PHP right after the PHP process
 *  has started, and that returns an address of an internal PHP
 *  strucure with all the details and features of your extension
 *
 *  @return void*   a pointer to an address that is understood by PHP
 */
PHPCPP_EXPORT void *get_module() {
    // static(!) Php::Extension object that should stay in memory
    // for the entire duration of the process (that's why it's static)
    static Php::Extension extension("service-discovery", "1.0");
//    static int s = zk->getState();
//    Php::out << "connected to zk" + s << std::endl;
//    Php::out << zk->message(1) << std::endl;
//    Php::out.flush();
    // @todo    add your own functions, classes, namespaces to the extension
    extension.add("get_service", getService, {
            Php::ByVal("service_name", Php::Type::String, true)
    });

    extension.onShutdown([]() {
        Php::out << "shutting down" << std::endl;
        services.clear();
    });

    extension.add(Php::Ini(Config_Servers_Key, "notexists:2181"));
    extension.onStartup([]() {
        std::string servers = Php::ini_get(Config_Servers_Key);
        Php::out << "on starting up, connecting to servers " << servers << std::endl;
        ZooKeeperStorageProcess *process = new ZooKeeperStorageProcess(servers, Duration::create(60).get(), "/", &services);
        spawn(process);
//        no need to initialize the process as it seems spawn will do it
//        process->initialize();
        //initialize all values through event func
    });

    // return the extension
    return extension;
    }
}