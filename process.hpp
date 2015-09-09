/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License
*/

#include <google/protobuf/message.h>
#include <google/protobuf/io/zero_copy_stream_impl.h> // For ArrayInputStream.

#include <queue>
#include <set>
#include <string>
#include <vector>

#include <process/dispatch.hpp>
#include <process/future.hpp>
#include <process/process.hpp>

#include <stout/duration.hpp>
#include <stout/error.hpp>
#include <stout/none.hpp>
#include <stout/option.hpp>
#include <stout/result.hpp>
#include <stout/some.hpp>
#include <stout/strings.hpp>
#include <stout/try.hpp>
#include <stout/uuid.hpp>

#include "watcher.hpp"
#include "zookeeper.hpp"

using namespace process;

// Note that we don't add 'using std::set' here because we need
// 'std::' to disambiguate the 'set' member.
using std::queue;
using std::string;
using std::vector;

const string SERVICE_PATH_PREFIX = "/nerve/services";
const string CONFIG_HOST = "host";
const string CONFIG_PORT = "port";
const string CONFIG_NAME = "name";
const string CONFIG_WEIGHT = "weight";

class ZooKeeperStorageProcess : public Process<ZooKeeperStorageProcess> {
public:
    ZooKeeperStorageProcess(
            const string &servers,
            const Duration &timeout,
            const string &znode,
            std::map<std::string, Php::Value> *services);

    virtual ~ZooKeeperStorageProcess();

    virtual void initialize();

    void addNewNode(const string &serviceName, const string &path);

    void removeNode(const string &path);

    void addNewService(const string &path);

    // ZooKeeper events.
    // Note that events from previous sessions are dropped.
    void connected(int64_t sessionId, bool reconnect);

    void reconnecting(int64_t sessionId);

    void expired(int64_t sessionId);

    void updated(int64_t sessionId, const string &path);

    void created(int64_t sessionId, const string &path);

    void deleted(int64_t sessionId, const string &path);

private:
    const string servers;

    // The session timeout requested by the client.
    const Duration timeout;

    const string znode;

    Watcher *watcher;
    ZooKeeper *zk;
    std::map<std::string, Php::Value> *services;

    // ZooKeeper connection state.
    enum State {
        DISCONNECTED,
        CONNECTING,
        CONNECTED,
    } state;
};

ZooKeeperStorageProcess::ZooKeeperStorageProcess(
        const string &_servers,
        const Duration &_timeout,
        const string &_znode,
        std::map<std::string, Php::Value> *_services)
        : servers(_servers),
          timeout(_timeout),
          znode(strings::remove(_znode, "/", strings::SUFFIX)),
          watcher(NULL),
          zk(NULL),
          services(_services),
          state(DISCONNECTED) { }

ZooKeeperStorageProcess::~ZooKeeperStorageProcess() {
    delete zk;
    delete watcher;
}

const string currentDateTime() {
    time_t now = time(0);
    struct tm tstruct;
    char buf[80];
    tstruct = *localtime(&now);
    // Visit http://en.cppreference.com/w/cpp/chrono/c/strftime
    // for more information about date/time format
    strftime(buf, sizeof(buf), "%Y-%m-%d.%X", &tstruct);

    return buf;
}

const string getLogPrefix() {
    return currentDateTime() + ": SERVICE_DISCOVERY: ";
}

void log(const string &message) {
    Php::out << getLogPrefix() << message << std::endl;
}

void log(const string &serviceName, const string &nodeName, const string &message){
    Php::out << getLogPrefix() << serviceName << ": " << nodeName << ": " << message << std::endl;
}

Php::Value parseConfig(const string &instanceConfig) {
    picojson::value value;
    Php::Value configValue;
    std::istringstream is(instanceConfig);
    string err = picojson::parse(value, is);
    if (err.empty()) {
        if (!value.contains(CONFIG_HOST) || !value.contains(CONFIG_PORT)) {
            string message = "config host or port is not found, skipping";
            log(message);
            return message;
        }
        configValue[CONFIG_HOST] = value.get(CONFIG_HOST).to_str();
        picojson::value port = value.get(CONFIG_PORT);
        if (port.is<int>()) {
            configValue[CONFIG_PORT] = (int) value.get(CONFIG_PORT).get<double>();
        } else {
            string message = "invalid config value port, skipping this instance";
            log(message);
            return message;
        }
        configValue[CONFIG_NAME] = value.get(CONFIG_NAME).to_str();
        picojson::value weight = value.get(CONFIG_WEIGHT);
        if (weight.is<int>()) {
            configValue[CONFIG_WEIGHT] = (int) weight.get<double>();
        }
        return configValue;
    }

    return err;
}

vector<string> split(const string &input, string delim) {
    vector<string> tokens;
    auto start = 0U;
    auto end = input.find(delim);
    while (end != string::npos) {
        tokens.push_back(input.substr(start, end - start));
        start = end + delim.length();
        end = input.find(delim, start);
    }

    tokens.push_back(input.substr(start, end - start));
    return tokens;
}

string getServiceName(const string &path) {
    auto tokens = split(path, "/");
    return tokens[3];
}

string getNodeName(const string &path) {
    auto tokens = split(path, "/");
    return tokens.back();
}

const int SERVICE_PATH_DEPTH = 3;
const int SERVICE_NODE_PATH_DEPTH = 5;

bool isServicePath(const string &path) {
    auto tokens = split(path, "/");
    if (tokens.size() == SERVICE_PATH_DEPTH) {
        return true;
    }

    return false;
}

bool isNodePath(const string &path) {
    auto tokens = split(path, "/");
    if (tokens.size() == SERVICE_NODE_PATH_DEPTH) {
        return true;
    }

    return false;
}

void ZooKeeperStorageProcess::initialize() {
    // Doing initialization here allows to avoid the race between
    // instantiating the ZooKeeper instance and being spawned ourself.
    watcher = new ProcessWatcher<ZooKeeperStorageProcess>(self());
    zk = new ZooKeeper(servers, timeout, watcher);
}

void ZooKeeperStorageProcess::removeNode(const string &path) {
    auto serviceName = getServiceName(path);
    std::map<string, Php::Value>::iterator find = services->find(serviceName);
    if (find == services->end()) {
        return;
    } else {
        auto nodeName = getNodeName(path);
        find->second.unset(nodeName);
        log(serviceName, nodeName, "removed");
    }
}

void ZooKeeperStorageProcess::addNewNode(const string &serviceName, const string &path) {
    string config;
    int code = zk->get(path, true, &config, NULL);
    if (code == ZOK) {
        Php::Value configValue = parseConfig(config);
        auto nodeName = getNodeName(path);
        if (configValue.isString()) {
            log(serviceName, nodeName,  "instance config is invalid json");
        } else {
            log(serviceName, nodeName, "added " + config);
            std::map<string, Php::Value>::iterator find = services->find(serviceName);
            if (find == services->end()) {
                Php::Array instances;
                instances.set(nodeName, configValue);
                (*services)[serviceName] = instances;
            } else {
                find->second.set(nodeName, configValue);
            }
        }
    }
}

void ZooKeeperStorageProcess::addNewService(const string &path) {
    vector<string> childs;
    int code = zk->getChildren(path, true, &childs);
    string serviceName = getServiceName(path);
    if (code == ZOK) {
        for (auto &child : childs) {
            addNewNode(serviceName, path + "/" + child);
        }
    }
}

void ZooKeeperStorageProcess::connected(int64_t sessionId, bool reconnect) {
    if (sessionId != zk->getSessionId()) {
        return;
    }
    log("connected, initilizing config values...");
    //get all service config
    vector<string> serviceNames;
    int code;
    code = zk->getChildren(SERVICE_PATH_PREFIX, true, &serviceNames);
    if (code == ZOK) {
        //init the global config object here
        for (auto &serviceName : serviceNames) {
            string servicePath = SERVICE_PATH_PREFIX + "/" + serviceName + "/services";
            addNewService(servicePath);
            }
//            (*services)[serviceName] = instances;
    } else {
        log("no config values found on path " + SERVICE_PATH_PREFIX);
        log("using cached config file from runtime folder");
    };
    state = CONNECTED;
}

void ZooKeeperStorageProcess::reconnecting(int64_t sessionId) {
    if (sessionId != zk->getSessionId()) {
        return;
    }
    log("session dropped, reconnecting...");
    state = CONNECTING;
}

void ZooKeeperStorageProcess::expired(int64_t sessionId) {
    if (sessionId != zk->getSessionId()) {
        return;
    }

    log("session expired, trying new session...");
    state = DISCONNECTED;

    delete zk;
    zk = new ZooKeeper(servers, timeout, watcher);

    state = CONNECTING;
}

void ZooKeeperStorageProcess::updated(int64_t sessionId, const string &path) {
    log("node " + path + " updated");
    vector<string> childs;
    int code = zk->getChildren(path, true, &childs);
    if (code == ZOK) {
        auto serviceName = getServiceName(path);
        std::map<string, Php::Value>::iterator find = services->find(serviceName);
        for (auto &child : childs) {
            if (find==services->end() || !find->second.contains(child)) {
                addNewNode(serviceName, path + "/" + child);
            }
        }
    }
}

void ZooKeeperStorageProcess::created(int64_t sessionId, const string &path) {
    log("new node " + path + " created");
}

void ZooKeeperStorageProcess::deleted(int64_t sessionId, const string &path) {
    log("node " + path + " deleted");
    removeNode(path);
}