/**
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "lgraph/common/namespace.h"
#include "lgraph/proto/write_service.grpc.pb.h"

namespace gsw = gs::rpc::write_service::v1;

namespace LGRAPH_NAMESPACE {
namespace client {

class BatchBuilder {
public:
  using PropertyMap = std::unordered_map<std::string, std::string>;

  BatchBuilder() : bwr_(), record_num_(0) {}

  void AddVertex(const std::string& label, const std::string& id, const PropertyMap& prop_map);
  void AddEdge(const std::string& label, int64_t edge_inner_id,
               const std::string& src_label, const std::string& src_id,
               const std::string& dst_label, const std::string& dst_id,
               const PropertyMap& prop_map);
  gsw::BatchWriteRequest AsRequest(const std::string& client_id = "DEFAULT");
  uint32_t Size() const { return record_num_; }
  void Clear();

private:
  gsw::BatchWriteRequest bwr_;
  uint32_t record_num_;
};

inline
void BatchBuilder::AddVertex(const std::string& label, const std::string& id, const PropertyMap& prop_map) {
  auto* vrk = new gsw::VertexRecordKeyPb;
  vrk->set_label(label);
  vrk->mutable_pk_properties()->insert({"id", id});
  auto* dr = new gsw::DataRecordPb;
  dr->set_allocated_vertex_record_key(vrk);
  auto& props = *(dr->mutable_properties());
  for (auto& entry : prop_map) {
    props[entry.first] = entry.second;
  }
  auto* wr = bwr_.add_write_requests();
  wr->set_write_type(gsw::INSERT);
  wr->set_allocated_data_record(dr);
  record_num_++;
}

inline
void BatchBuilder::AddEdge(const std::string& label, int64_t edge_inner_id,
                           const std::string& src_label, const std::string& src_id,
                           const std::string& dst_label, const std::string& dst_id,
                           const PropertyMap& prop_map) {
  auto* src_vrk = new gsw::VertexRecordKeyPb;
  src_vrk->set_label(src_label);
  src_vrk->mutable_pk_properties()->insert({"id", src_id});
  auto* dst_vrk = new gsw::VertexRecordKeyPb;
  dst_vrk->set_label(dst_label);
  dst_vrk->mutable_pk_properties()->insert({"id", dst_id});
  auto* erk = new gsw::EdgeRecordKeyPb;
  erk->set_label(label);
  erk->set_allocated_src_vertex_key(src_vrk);
  erk->set_allocated_dst_vertex_key(dst_vrk);
  erk->set_inner_id(edge_inner_id);
  auto* dr = new gsw::DataRecordPb;
  dr->set_allocated_edge_record_key(erk);
  auto& props = *(dr->mutable_properties());
  for (auto& entry : prop_map) {
    props[entry.first] = entry.second;
  }
  auto* wr = bwr_.add_write_requests();
  wr->set_write_type(gsw::INSERT);
  wr->set_allocated_data_record(dr);
  record_num_++;
}

inline
gsw::BatchWriteRequest BatchBuilder::AsRequest(const std::string& client_id) {
  bwr_.set_client_id(client_id);
  auto batch = std::move(bwr_);
  record_num_ = 0;
  return batch;
}

inline
void BatchBuilder::Clear() {
  bwr_.clear_write_requests();
  record_num_ = 0;
}

}
}
