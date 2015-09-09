#include <phpcpp.h>
#include <ostream>
#include "zookeeper.hpp"
#include "process.hpp"

const char *Config_Servers_Key = "service-discovery.servers";
std::map<std::string, Php::Value> services;
ZooKeeperStorageProcess *zkProcess;

Php::Array map2Array(std::map<string, Php::Value> &values) {
    Php::Array array;
    for (std::map<std::string, Php::Value>::iterator iter = values.begin(); iter != values.end(); ++iter) {
        array[iter->first] = iter->second;
    }
    return array;
}

Php::Value next(Php::Value &service) {
    int totalWeight = 0;
    for (Php::Value::iterator iter = service.begin(); iter != service.end(); ++iter) {
        if (!iter->second.contains(CONFIG_WEIGHT)) {
            totalWeight = 0;
            break;
        }
        int weight = iter->second.get(CONFIG_WEIGHT);
        totalWeight += weight;
    }

    int r = rand();
    if (totalWeight == 0) {
        auto step = r % service.count();
        Php::Value::iterator iter = service.begin();
        for(int i=0;i<step;i++){
            iter++;
        }
        return iter->second;
    }

    int calculatedWeight = 0;
    int i = r % totalWeight;
    for (Php::Value::iterator iter = service.begin(); iter != service.end(); ++iter) {
        int weight = iter->second.get(CONFIG_WEIGHT);
        if (calculatedWeight <= i && i < calculatedWeight + weight) {
            return iter->second;
        }
        calculatedWeight += weight;
    }

    return false;
}

Php::Value findService(std::string &serviceName) {
    std::map<std::string, Php::Value>::iterator find = services.find(serviceName);
    if (find != services.end()) {
        return find->second;
    }

    return false;
}

Php::Value getService(Php::Parameters &params) {
    string serviceName = params[0];
    return findService(serviceName);
}

Php::Value getOneService(Php::Parameters &params) {
    string serviceName = params[0];
    Php::Value service = findService(serviceName);
    if (service == false || service.count() == 0) {
        return false;
    }
    return next(service);
}

Php::Value getAllService() {
    return map2Array(services);
}

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

    // @todo    add your own functions, classes, namespaces to the extension
    extension.add("service_discovery_get_all", getAllService);

    extension.add("service_discovery_get", getService, {
            Php::ByVal("service_name", Php::Type::String, true)
    });


    extension.add("service_discovery_get_one", getOneService, {
            Php::ByVal("service_name", Php::Type::String, true)
    });

    extension.onShutdown([]() {
        Php::out << "shutting down" << std::endl;
        services.clear();
        delete zkProcess;
    });

    extension.add(Php::Ini(Config_Servers_Key, "notexists:2181"));
    extension.onStartup([]() {
        std::string servers = Php::ini_get(Config_Servers_Key);
        log("on starting up, connecting to servers " + servers);
        zkProcess = new ZooKeeperStorageProcess(servers, Duration::create(60).get(), "/",
                                                                       &services);
        spawn(zkProcess);
        //initialize all values through event func
    });

    // return the extension
    return extension;
}
}