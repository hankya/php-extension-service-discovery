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

class ZooKeeperStorageProcess : public Process<ZooKeeperStorageProcess> {
public:
    ZooKeeperStorageProcess(
            const string &servers,
            const Duration &timeout,
            const string &znode,
            std::map<std::string, Php::Value>* services);

    virtual ~ZooKeeperStorageProcess();

    virtual void initialize();

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


void ZooKeeperStorageProcess::initialize() {
    // Doing initialization here allows to avoid the race between
    // instantiating the ZooKeeper instance and being spawned ourself.
    watcher = new ProcessWatcher<ZooKeeperStorageProcess>(self());
    zk = new ZooKeeper(servers, timeout, watcher);
}

void ZooKeeperStorageProcess::connected(int64_t sessionId, bool reconnect) {
    if (sessionId != zk->getSessionId()) {
        return;
    }
    Php::out << "connected" << std::endl;
    std::string result;
    int code = zk->get("/nerve/services/hello/services/localhost_0000000109", false, &result, NULL);
    Php::out << "code is " << code << " ; and result is " << result << std::endl;
    Php::Value v = result;
    std::string hello= "hello";
    (*services)[hello] = v;
    Php::out << "hello is " << (*services)[hello] << std::endl;
    state = CONNECTED;
}

void ZooKeeperStorageProcess::reconnecting(int64_t sessionId) {
    if (sessionId != zk->getSessionId()) {
        return;
    }

    Php::out << "reconnecting" << std::endl;
    state = CONNECTING;
}


void ZooKeeperStorageProcess::expired(int64_t sessionId) {
    if (sessionId != zk->getSessionId()) {
        return;
    }

    Php::out << "disconnected" << std::endl;
    state = DISCONNECTED;

    delete zk;
    zk = new ZooKeeper(servers, timeout, watcher);

    state = CONNECTING;
}


void ZooKeeperStorageProcess::updated(int64_t sessionId, const string &path) {
    LOG(FATAL) << "Unexpected ZooKeeper event";
}


void ZooKeeperStorageProcess::created(int64_t sessionId, const string &path) {
    LOG(FATAL) << "Unexpected ZooKeeper event";
}


void ZooKeeperStorageProcess::deleted(int64_t sessionId, const string &path) {
    LOG(FATAL) << "Unexpected ZooKeeper event";
}