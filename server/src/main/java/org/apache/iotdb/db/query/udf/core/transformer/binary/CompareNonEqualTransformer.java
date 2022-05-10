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

package org.apache.iotdb.db.query.udf.core.transformer.binary;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class CompareNonEqualTransformer extends CompareBinaryTransformer {

  public CompareNonEqualTransformer(
      LayerPointReader leftPointReader, LayerPointReader rightPointReader)
      throws QueryProcessException {
    super(leftPointReader, rightPointReader);
  }

  @Override
  protected void checkType() throws QueryProcessException {
    if ((leftPointReaderDataType == TSDataType.BOOLEAN
            && rightPointReaderDataType != TSDataType.BOOLEAN)
        || (leftPointReaderDataType != TSDataType.BOOLEAN
            && rightPointReaderDataType == TSDataType.BOOLEAN)) {
      throw new QueryProcessException(
          "Data type doesn't match on both sides of '=', left: "
              + leftPointReaderDataType.toString()
              + ", right: "
              + rightPointReaderDataType.toString());
    }
  }

  @Override
  protected boolean evaluate(double leftOperand, double rightOperand) {
    return Double.compare(leftOperand, rightOperand) != 0;
  }
}
