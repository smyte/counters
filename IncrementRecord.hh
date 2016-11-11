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
 * limitations under the License.
 */


#ifndef COUNTERS_INCREMENTRECORD_HH_415392616__H_
#define COUNTERS_INCREMENTRECORD_HH_415392616__H_


#include <sstream>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include "boost/any.hpp"
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"
#pragma GCC diagnostic pop

namespace counters {
struct increment {
    int32_t v;
    int64_t timeMs;
    int64_t datasetId;
    std::string key;
    int32_t by;
    increment() :
        v(int32_t()),
        timeMs(int64_t()),
        datasetId(int64_t()),
        key(std::string()),
        by(int32_t())
        { }
};

}
namespace avro {
template<> struct codec_traits<counters::increment> {
    static void encode(Encoder& e, const counters::increment& v) {
        avro::encode(e, v.v);
        avro::encode(e, v.timeMs);
        avro::encode(e, v.datasetId);
        avro::encode(e, v.key);
        avro::encode(e, v.by);
    }
    static void decode(Decoder& d, counters::increment& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.v);
                    break;
                case 1:
                    avro::decode(d, v.timeMs);
                    break;
                case 2:
                    avro::decode(d, v.datasetId);
                    break;
                case 3:
                    avro::decode(d, v.key);
                    break;
                case 4:
                    avro::decode(d, v.by);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.v);
            avro::decode(d, v.timeMs);
            avro::decode(d, v.datasetId);
            avro::decode(d, v.key);
            avro::decode(d, v.by);
        }
    }
};

}
#endif
