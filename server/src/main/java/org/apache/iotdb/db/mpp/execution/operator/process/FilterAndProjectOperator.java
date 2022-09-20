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

package org.apache.iotdb.db.mpp.execution.operator.process;

import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.CodegenContext;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.CodegenEvaluator;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.CodegenEvaluatorImpl;
import org.apache.iotdb.db.mpp.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.binary.BinaryColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.leaf.ConstantColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.leaf.TimeColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.multi.MappableUDFColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.ternary.TernaryColumnTransformer;
import org.apache.iotdb.db.mpp.transformation.dag.column.unary.UnaryColumnTransformer;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;

public class FilterAndProjectOperator implements ProcessOperator {

  private final Operator inputOperator;

  private List<LeafColumnTransformer> filterLeafColumnTransformerList;

  private ColumnTransformer filterOutputTransformer;

  private List<ColumnTransformer> commonTransformerList;

  private List<LeafColumnTransformer> projectLeafColumnTransformerList;

  private List<ColumnTransformer> projectOutputTransformerList;

  private final TsBlockBuilder filterTsBlockBuilder;

  private final boolean hasNonMappableUDF;

  private final OperatorContext operatorContext;

  // false when we only need to do projection
  private final boolean hasFilter;
  private boolean codegenSuccess;

  private boolean hasCodegenEvaluatedCache;

  private Column[] codegenEvaluatedColumns;

  private CodegenEvaluator codegenEvaluator;

  public FilterAndProjectOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> filterOutputDataTypes,
      List<LeafColumnTransformer> filterLeafColumnTransformerList,
      ColumnTransformer filterOutputTransformer,
      List<ColumnTransformer> commonTransformerList,
      List<LeafColumnTransformer> projectLeafColumnTransformerList,
      List<ColumnTransformer> projectOutputTransformerList,
      boolean hasNonMappableUDF,
      boolean hasFilter) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.filterLeafColumnTransformerList = filterLeafColumnTransformerList;
    this.filterOutputTransformer = filterOutputTransformer;
    this.commonTransformerList = commonTransformerList;
    this.projectLeafColumnTransformerList = projectLeafColumnTransformerList;
    this.projectOutputTransformerList = projectOutputTransformerList;
    this.hasNonMappableUDF = hasNonMappableUDF;
    this.filterTsBlockBuilder = new TsBlockBuilder(8, filterOutputDataTypes);
    this.hasFilter = hasFilter;
    codegenSuccess = false;
  }

  public FilterAndProjectOperator(
      CodegenContext codegenContext,
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> filterOutputDataTypes,
      List<LeafColumnTransformer> filterLeafColumnTransformerList,
      ColumnTransformer filterOutputTransformer,
      List<ColumnTransformer> commonTransformerList,
      List<LeafColumnTransformer> projectLeafColumnTransformerList,
      List<ColumnTransformer> projectOutputTransformerList,
      boolean hasNonMappableUDF,
      boolean hasFilter) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.filterLeafColumnTransformerList = filterLeafColumnTransformerList;
    this.filterOutputTransformer = filterOutputTransformer;
    this.commonTransformerList = commonTransformerList;
    this.projectLeafColumnTransformerList = projectLeafColumnTransformerList;
    this.projectOutputTransformerList = projectOutputTransformerList;
    this.hasNonMappableUDF = hasNonMappableUDF;
    this.filterTsBlockBuilder = new TsBlockBuilder(8, filterOutputDataTypes);
    this.hasFilter = hasFilter;
    tryCodegen(codegenContext);
  }

  private void tryCodegen(CodegenContext codegenContext) {
    codegenEvaluator = new CodegenEvaluatorImpl(codegenContext);
    try {
      codegenEvaluator.generateEvaluatorClass();
      codegenSuccess = true;
    } catch (Exception e) {
      codegenSuccess = false;
    }
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    TsBlock input = inputOperator.next();
    if (input == null) {
      return null;
    }

    if (!hasFilter) {
      return getTransformedTsBlock(input);
    }

    TsBlock filterResult = getFilterTsBlock(input);

    // contains non-mappable udf, we leave calculation for TransformOperator
    if (hasNonMappableUDF) {
      return filterResult;
    }
    return getTransformedTsBlock(filterResult);
  }

  /**
   * Return the TsBlock that contains both initial input columns and columns of common
   * subexpressions after filtering
   *
   * @param input
   * @return
   */
  private TsBlock getFilterTsBlock(TsBlock input) {
    final TimeColumn originTimeColumn = input.getTimeColumn();
    final int positionCount = originTimeColumn.getPositionCount();
    // feed Filter ColumnTransformer, including TimeStampColumnTransformer and constant
    for (LeafColumnTransformer leafColumnTransformer : filterLeafColumnTransformerList) {
      leafColumnTransformer.initFromTsBlock(input);
    }

    filterOutputTransformer.tryEvaluate();

    Column filterColumn = filterOutputTransformer.getColumn();

    // reuse this builder
    filterTsBlockBuilder.reset();

    final TimeColumnBuilder timeBuilder = filterTsBlockBuilder.getTimeColumnBuilder();
    final ColumnBuilder[] columnBuilders = filterTsBlockBuilder.getValueColumnBuilders();

    List<Column> resultColumns = new ArrayList<>();
    for (int i = 0, n = input.getValueColumnCount(); i < n; i++) {
      resultColumns.add(input.getColumn(i));
    }

    // todo: remove this if, add calculated common sub expressions anyway
    if (!hasNonMappableUDF) {
      // get result of calculated common sub expressions
      for (ColumnTransformer columnTransformer : commonTransformerList) {
        resultColumns.add(columnTransformer.getColumn());
      }
    }

    // construct result TsBlock of filter
    int rowCount = 0;
    for (int i = 0, n = resultColumns.size(); i < n; i++) {
      Column curColumn = resultColumns.get(i);
      for (int j = 0; j < positionCount; j++) {
        if (!filterColumn.isNull(j) && filterColumn.getBoolean(j)) {
          if (i == 0) {
            rowCount++;
            timeBuilder.writeLong(originTimeColumn.getLong(j));
          }
          if (curColumn.isNull(j)) {
            columnBuilders[i].appendNull();
          } else {
            columnBuilders[i].write(curColumn, j);
          }
        }
      }
    }

    filterTsBlockBuilder.declarePositions(rowCount);
    return filterTsBlockBuilder.build();
  }

  private TsBlock getTransformedTsBlock(TsBlock input) {
    final TimeColumn originTimeColumn = input.getTimeColumn();
    final int positionCount = originTimeColumn.getPositionCount();
    // feed pre calculated data
    for (LeafColumnTransformer leafColumnTransformer : projectLeafColumnTransformerList) {
      leafColumnTransformer.initFromTsBlock(input);
    }

    hasCodegenEvaluatedCache = false;
    List<Column> resultColumns = new ArrayList<>();
    for (int i = 0; i < projectOutputTransformerList.size(); ++i) {
      Column outputColumn;
      outputColumn = tryGetColumnByCodegen(input, i);
      if (outputColumn == null) {
        ColumnTransformer columnTransformer = projectOutputTransformerList.get(i);
        columnTransformer.tryEvaluate();
        outputColumn = columnTransformer.getColumn();
      }
      resultColumns.add(outputColumn);
    }
    return TsBlock.wrapBlocksWithoutCopy(
        positionCount, originTimeColumn, resultColumns.toArray(new Column[0]));
  }

  private Column tryGetColumnByCodegen(TsBlock input, int i) {
    if (codegenSuccess) {
      if (!hasCodegenEvaluatedCache) {
        codegenEvaluatedColumns = codegenEvaluator.evaluate(input);
        hasCodegenEvaluatedCache = true;
      }
      return codegenEvaluatedColumns[i];
    }
    return null;
  }

  @Override
  public boolean hasNext() {
    return inputOperator.hasNext();
  }

  @Override
  public boolean isFinished() {
    return inputOperator.isFinished();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return inputOperator.isBlocked();
  }

  @Override
  public void close() throws Exception {
    for (ColumnTransformer columnTransformer : projectOutputTransformerList) {
      columnTransformer.close();
    }
    if (filterOutputTransformer != null) {
      filterOutputTransformer.close();
    }
    inputOperator.close();
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemory = inputOperator.calculateMaxReturnSize();
    int maxCachedColumn = 0;
    // Only do projection, calculate max cached column size of calc tree
    if (!hasFilter) {
      for (int i = 0; i < projectOutputTransformerList.size(); i++) {
        ColumnTransformer c = projectOutputTransformerList.get(i);
        maxCachedColumn = Math.max(maxCachedColumn, 1 + i + getMaxLevelOfColumnTransformerTree(c));
      }
      return Math.max(
              maxPeekMemory,
              (long) maxCachedColumn
                  * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte())
          + inputOperator.calculateRetainedSizeAfterCallingNext();
    }

    // has Filter
    maxCachedColumn =
        Math.max(
            1 + getMaxLevelOfColumnTransformerTree(filterOutputTransformer),
            1 + commonTransformerList.size());
    if (!hasNonMappableUDF) {
      for (int i = 0; i < projectOutputTransformerList.size(); i++) {
        ColumnTransformer c = projectOutputTransformerList.get(i);
        maxCachedColumn = Math.max(maxCachedColumn, 1 + i + getMaxLevelOfColumnTransformerTree(c));
      }
    }
    return Math.max(
            maxPeekMemory,
            (long) maxCachedColumn * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte())
        + inputOperator.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long calculateMaxReturnSize() {
    // time + all value columns
    if (!hasFilter || !hasNonMappableUDF) {
      return (long) (1 + projectOutputTransformerList.size())
          * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    } else {
      return (long) (1 + filterTsBlockBuilder.getValueColumnBuilders().length)
          * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    }
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return inputOperator.calculateRetainedSizeAfterCallingNext();
  }

  private int getMaxLevelOfColumnTransformerTree(ColumnTransformer columnTransformer) {
    if (columnTransformer instanceof LeafColumnTransformer) {
      // Time column is always calculated, we ignore it here. Constant column is ignored.
      if (columnTransformer instanceof ConstantColumnTransformer
          || columnTransformer instanceof TimeColumnTransformer) {
        return 0;
      } else {
        return 1;
      }
    } else if (columnTransformer instanceof UnaryColumnTransformer) {
      return Math.max(
          2,
          getMaxLevelOfColumnTransformerTree(
              ((UnaryColumnTransformer) columnTransformer).getChildColumnTransformer()));
    } else if (columnTransformer instanceof BinaryColumnTransformer) {
      int childMaxLevel =
          Math.max(
              getMaxLevelOfColumnTransformerTree(
                  ((BinaryColumnTransformer) columnTransformer).getLeftTransformer()),
              getMaxLevelOfColumnTransformerTree(
                  ((BinaryColumnTransformer) columnTransformer).getRightTransformer()));
      return Math.max(3, childMaxLevel);
    } else if (columnTransformer instanceof TernaryColumnTransformer) {
      int childMaxLevel =
          Math.max(
              getMaxLevelOfColumnTransformerTree(
                  ((TernaryColumnTransformer) columnTransformer).getFirstColumnTransformer()),
              Math.max(
                  getMaxLevelOfColumnTransformerTree(
                      ((TernaryColumnTransformer) columnTransformer).getSecondColumnTransformer()),
                  getMaxLevelOfColumnTransformerTree(
                      ((TernaryColumnTransformer) columnTransformer).getThirdColumnTransformer())));
      return Math.max(4, childMaxLevel);
    } else if (columnTransformer instanceof MappableUDFColumnTransformer) {
      int childMaxLevel = 0;
      for (ColumnTransformer c :
          ((MappableUDFColumnTransformer) columnTransformer).getInputColumnTransformers()) {
        childMaxLevel = Math.max(childMaxLevel, getMaxLevelOfColumnTransformerTree(c));
      }
      return Math.max(
          1
              + ((MappableUDFColumnTransformer) columnTransformer)
                  .getInputColumnTransformers()
                  .length,
          childMaxLevel);
    } else {
      throw new UnsupportedOperationException("Unsupported ColumnTransformer");
    }
  }
}
