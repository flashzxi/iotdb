/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.confignode.physical.sys;

import org.apache.iotdb.commons.utils.ThriftConfigNodeSerDeUtils;
import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.physical.PhysicalPlanType;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class SetStorageGroupPlan extends PhysicalPlan {

  private TStorageGroupSchema schema;

  public SetStorageGroupPlan() {
    super(PhysicalPlanType.SetStorageGroup);
    this.schema = new TStorageGroupSchema();
  }

  public SetStorageGroupPlan(TStorageGroupSchema schema) {
    this();
    this.schema = schema;
  }

  public TStorageGroupSchema getSchema() {
    return schema;
  }

  public void setSchema(TStorageGroupSchema schema) {
    this.schema = schema;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(PhysicalPlanType.SetStorageGroup.ordinal());
    ThriftConfigNodeSerDeUtils.writeTStorageGroupSchema(schema, buffer);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    schema = ThriftConfigNodeSerDeUtils.readTStorageGroupSchema(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SetStorageGroupPlan that = (SetStorageGroupPlan) o;
    return schema.equals(that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema);
  }
}
