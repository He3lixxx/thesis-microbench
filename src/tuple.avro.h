/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#ifndef TUPLE_AVRO_H_815300698__H_
#define TUPLE_AVRO_H_815300698__H_


#include <sstream>
#include "boost/any.hpp"
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"

namespace bench_avro {
struct Tuple {
    int64_t id;
    int64_t timestamp;
    float load;
    float load_avg_1;
    float load_avg_5;
    float load_avg_15;
    std::array<uint8_t, 32> container_id;
    Tuple() :
        id(int64_t()),
        timestamp(int64_t()),
        load(float()),
        load_avg_1(float()),
        load_avg_5(float()),
        load_avg_15(float()),
        container_id(std::array<uint8_t, 32>())
        { }
};

}
namespace avro {
template<> struct codec_traits<bench_avro::Tuple> {
    static void encode(Encoder& e, const bench_avro::Tuple& v) {
        avro::encode(e, v.id);
        avro::encode(e, v.timestamp);
        avro::encode(e, v.load);
        avro::encode(e, v.load_avg_1);
        avro::encode(e, v.load_avg_5);
        avro::encode(e, v.load_avg_15);
        avro::encode(e, v.container_id);
    }
    static void decode(Decoder& d, bench_avro::Tuple& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.id);
                    break;
                case 1:
                    avro::decode(d, v.timestamp);
                    break;
                case 2:
                    avro::decode(d, v.load);
                    break;
                case 3:
                    avro::decode(d, v.load_avg_1);
                    break;
                case 4:
                    avro::decode(d, v.load_avg_5);
                    break;
                case 5:
                    avro::decode(d, v.load_avg_15);
                    break;
                case 6:
                    avro::decode(d, v.container_id);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.id);
            avro::decode(d, v.timestamp);
            avro::decode(d, v.load);
            avro::decode(d, v.load_avg_1);
            avro::decode(d, v.load_avg_5);
            avro::decode(d, v.load_avg_15);
            avro::decode(d, v.container_id);
        }
    }
};

}
#endif
