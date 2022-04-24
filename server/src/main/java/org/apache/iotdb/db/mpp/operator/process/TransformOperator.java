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

package org.apache.iotdb.db.mpp.operator.process;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.operator.Operator;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.db.query.dataset.IUDFInputDataSet;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.udf.core.executor.UDTFContext;
import org.apache.iotdb.db.query.udf.core.layer.EvaluationDAGBuilder;
import org.apache.iotdb.db.query.udf.core.layer.RawQueryInputLayer;
import org.apache.iotdb.db.query.udf.core.layer.TsBlockInputDataSet;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.db.query.udf.service.UDFRegistrationService;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransformOperator implements ProcessOperator {

  protected static final int FETCH_SIZE = 10000;

  protected final float udfReaderMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfReaderMemoryBudgetInMB();
  protected final float udfTransformerMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfTransformerMemoryBudgetInMB();
  protected final float udfCollectorMemoryBudgetInMB =
      IoTDBDescriptor.getInstance().getConfig().getUdfCollectorMemoryBudgetInMB();

  protected final OperatorContext operatorContext;
  protected final Operator inputOperator;
  protected final List<TSDataType> inputDataTypes;
  protected final Expression[] outputExpressions;
  protected final UDTFContext udtfContext;
  protected final boolean keepNull;

  protected IUDFInputDataSet inputDataset;
  protected LayerPointReader[] transformers;
  protected TimeSelector timeHeap;
  protected List<TSDataType> outputDataTypes;

  public TransformOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> inputDataTypes,
      Expression[] outputExpressions,
      UDTFContext udtfContext,
      boolean keepNull)
      throws QueryProcessException, IOException {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.inputDataTypes = inputDataTypes;
    this.outputExpressions = outputExpressions;
    this.udtfContext = udtfContext;
    this.keepNull = keepNull;

    initInputDataset(inputDataTypes);
    initTransformers();
    initTimeHeap();
  }

  protected void initInputDataset(List<TSDataType> inputDataTypes) {
    inputDataset = new TsBlockInputDataSet(inputOperator, inputDataTypes);
  }

  protected void initTransformers() throws QueryProcessException, IOException {
    UDFRegistrationService.getInstance().acquireRegistrationLock();
    // This statement must be surrounded by the registration lock.
    UDFClassLoaderManager.getInstance().initializeUDFQuery(operatorContext.getOperatorId());
    try {
      // UDF executors will be initialized at the same time
      transformers =
          new EvaluationDAGBuilder(
                  operatorContext.getOperatorId(),
                  new RawQueryInputLayer(
                      operatorContext.getOperatorId(), udfReaderMemoryBudgetInMB, inputDataset),
                  outputExpressions,
                  udtfContext,
                  udfTransformerMemoryBudgetInMB + udfCollectorMemoryBudgetInMB)
              .buildLayerMemoryAssigner()
              .buildResultColumnPointReaders()
              .getOutputPointReaders();
    } finally {
      UDFRegistrationService.getInstance().releaseRegistrationLock();
    }
  }

  protected void initTimeHeap() throws QueryProcessException, IOException {
    timeHeap = new TimeSelector(transformers.length << 1, true);
    for (LayerPointReader reader : transformers) {
      iterateReaderToNextValid(reader);
    }
  }

  protected void iterateReaderToNextValid(LayerPointReader reader)
      throws QueryProcessException, IOException {
    // Since a constant operand is not allowed to be a result column, the reader will not be
    // a ConstantLayerPointReader.
    // If keepNull is false, we must iterate the reader until a non-null row is returned.
    while (reader.next()) {
      if (reader.isCurrentNull() && !keepNull) {
        reader.readyForNext();
        continue;
      }
      timeHeap.add(reader.currentTime());
      break;
    }
  }

  @Override
  public boolean hasNext() {
    return !timeHeap.isEmpty();
  }

  @Override
  public TsBlock next() {
    final TsBlockBuilder tsBlockBuilder = TsBlockBuilder.createWithOnlyTimeColumn();

    if (outputDataTypes == null) {
      outputDataTypes = new ArrayList<>();
      for (LayerPointReader reader : transformers) {
        outputDataTypes.add(reader.getDataType());
      }
    }
    tsBlockBuilder.buildValueColumnBuilders(outputDataTypes);

    final TimeColumnBuilder timeBuilder = tsBlockBuilder.getTimeColumnBuilder();
    final ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    final int columnCount = columnBuilders.length;

    int rowCount = 0;
    try {
      while (rowCount < FETCH_SIZE && !timeHeap.isEmpty()) {
        collectCurrentRow(timeBuilder, columnBuilders, timeHeap.pollFirst(), columnCount, true);
        ++rowCount;
      }
    } catch (Exception e) {
      // TODO: throw here?
      throw new RuntimeException(e);
    }

    return tsBlockBuilder.build();
  }

  protected void collectCurrentRow(
      TimeColumnBuilder timeBuilder,
      ColumnBuilder[] columnBuilders,
      long currentTime,
      int outputColumnCount,
      boolean enableTimeHeap)
      throws QueryProcessException, IOException {
    // time
    timeBuilder.writeLong(currentTime);

    // values
    for (int i = 0; i < outputColumnCount; ++i) {
      LayerPointReader reader = transformers[i];

      // currentTime is the minTime, any timestamp smaller than currentTime should be dropped
      while (reader.next() && reader.currentTime() < currentTime) {
        reader.readyForNext();
      }

      if (!reader.next() || reader.currentTime() != currentTime || reader.isCurrentNull()) {
        columnBuilders[i].appendNull();
        continue;
      }

      TSDataType type = reader.getDataType();
      switch (type) {
        case INT32:
          columnBuilders[i].writeInt(reader.currentInt());
          break;
        case INT64:
          columnBuilders[i].writeLong(reader.currentLong());
          break;
        case FLOAT:
          columnBuilders[i].writeFloat(reader.currentFloat());
          break;
        case DOUBLE:
          columnBuilders[i].writeDouble(reader.currentDouble());
          break;
        case BOOLEAN:
          columnBuilders[i].writeBoolean(reader.currentBoolean());
          break;
        case TEXT:
          columnBuilders[i].writeBinary(reader.currentBinary());
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", type));
      }

      reader.readyForNext();

      if (enableTimeHeap) {
        iterateReaderToNextValid(reader);
      }
    }
  }

  @Override
  public void close() throws Exception {
    udtfContext.finalizeUDFExecutors(operatorContext.getOperatorId());

    inputOperator.close();
  }

  @Override
  public ListenableFuture<Void> isBlocked() {
    return inputOperator.isBlocked();
  }

  @Override
  public boolean isFinished() {
    return !hasNext();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }
}
