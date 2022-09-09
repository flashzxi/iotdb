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

package org.apache.iotdb.db.mpp.execution.operator.process.codegen;

import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ConstantExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.NewObjectsStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.ReturnStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.Statement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.utils.CodegenSimpleRow;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFContext;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class CodegenContext {
  private List<Statement> codes;

  private final Map<String, List<InputLocation>> inputLocations;
  private final List<TSDataType> inputDataTypes;

  private Map<String, String> inputNameToVarName;

  private final List<Expression> outputExpression;

  private final Expression filterExpression;

  private Map<Expression, TSDataType> expressionToDataType;

  private Map<Expression, ExpressionNode> expressionToNode;

  private Map<String, UDTFExecutor> udtfExecutors;

  private Map<String, CodegenSimpleRow> udtfInputs;

  private final TypeProvider typeProvider;

  private UDTFContext udtfContext;

  public UDTFContext getUdtfContext() {
    return udtfContext;
  }

  public void setUdtfContext(UDTFContext udtfContext) {
    this.udtfContext = udtfContext;
  }

  public void setIsExpressionGeneratedSuccess(List<Boolean> isExpressionGeneratedSuccess) {
    this.isExpressionGeneratedSuccess = isExpressionGeneratedSuccess;
  }

  private List<Boolean> isExpressionGeneratedSuccess;

  private long uniqueIndex;

  public CodegenContext(
      Map<String, List<InputLocation>> inputLocations,
      List<TSDataType> inputDataTypes,
      List<Expression> outputExpressions,
      Expression filterExpression,
      TypeProvider typeProvider) {
    init();

    this.inputLocations = inputLocations;
    this.inputDataTypes = inputDataTypes;
    this.outputExpression = outputExpressions;
    this.filterExpression = filterExpression;
    this.typeProvider = typeProvider;
  }

  public void init() {
    this.codes = new ArrayList<>();
    this.expressionToNode = new HashMap<>();
    this.expressionToDataType = new HashMap<>();
    this.udtfInputs = new HashMap<>();
    this.udtfExecutors = new HashMap<>();
    this.inputNameToVarName = new HashMap<>();
  }

  public void addInputVarNameMap(String inputName, String varName) {
    inputNameToVarName.put(inputName, varName);
  }

  public String getVarName(String inputName) {
    return inputNameToVarName.get(inputName);
  }

  public Map<String, String> getInputNameToVarName() {
    return inputNameToVarName;
  }

  public boolean isExpressionExisted(Expression expression) {
    return expressionToNode.containsKey(expression);
  }

  public void addExpression(
      Expression expression, ExpressionNode ExpressionNode, TSDataType tsDataType) {
    if (!expressionToNode.containsKey(expression)) {
      expressionToNode.put(expression, ExpressionNode);
      expressionToDataType.put(expression, tsDataType);
    }
  }

  public ExpressionNode getExpressionNode(Expression expression) {
    if (expressionToNode.containsKey(expression)) {
      return expressionToNode.get(expression);
    }
    return null;
  }

  public void addUdtfExecutor(String executorName, UDTFExecutor executor) {
    udtfExecutors.put(executorName, executor);
  }

  public void addUdtfInput(String rowName, CodegenSimpleRow input) {
    if (Objects.isNull(udtfInputs)) {
      udtfInputs = new HashMap<>();
    }
    udtfInputs.put(rowName, input);
  }

  public void addCode(Statement statement) {
    codes.add(statement);
  }

  public String uniqueVarName() {
    return "var" + (uniqueIndex++);
  }

  public String uniqueVarName(String prefix) {
    return prefix + (uniqueIndex++);
  }

  public void addOutputExpr(Expression expression) {
    outputExpression.add(expression);
  }

  public void generateReturnFilter() {
    ReturnStatement returnStatement =
        new ReturnStatement(
            new ConstantExpressionNode(expressionToNode.get(filterExpression).getNodeName()));
    codes.add(returnStatement);
  }

  public void generateReturnStatement() {
    String retValueVarName = uniqueVarName();
    NewObjectsStatement retValueStatement =
        new NewObjectsStatement(retValueVarName, outputExpression.size());

    for (int i = 0; i < outputExpression.size(); ++i) {
      Expression expression = outputExpression.get(i);
      if (isExpressionGeneratedSuccess.get(i)) {
        retValueStatement.addRetValue(expressionToNode.get(expression));
      } else {
        retValueStatement.addRetValue(null);
      }
    }
    codes.add(retValueStatement);
    codes.add(new ReturnStatement(new ConstantExpressionNode(retValueVarName)));
  }

  public String toCode() {
    StringBuilder code = new StringBuilder();
    for (Statement statement : codes) {
      if (statement != null) code.append(statement.toCode());
    }
    return code.toString();
  }

  public static Class<?> tsDatatypeToClass(TSDataType tsDataType) {
    switch (tsDataType) {
      case INT32:
        return Integer.class;
      case INT64:
        return Long.class;
      case FLOAT:
        return Float.class;
      case DOUBLE:
        return Double.class;
      case BOOLEAN:
        return Boolean.class;
      case TEXT:
        return String.class;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported for codegen.", tsDataType));
    }
  }

  public Class<?> getExpressionType(Expression expression) {
    if (!expressionToDataType.containsKey(expression)) {
      return null;
    }
    return tsDatatypeToClass(expressionToDataType.get(expression));
  }

  public Map<String, UDTFExecutor> getUdtfExecutors() {
    return udtfExecutors;
  }

  public Map<String, CodegenSimpleRow> getUdtfInputs() {
    return udtfInputs;
  }

  public Map<String, List<InputLocation>> getInputLocations() {
    return inputLocations;
  }

  public List<TSDataType> getInputDataTypes() {
    return inputDataTypes;
  }

  public List<Expression> getOutputExpression() {
    return outputExpression;
  }

  public Expression getFilterExpression() {
    return filterExpression;
  }

  public TypeProvider getTypeProvider() {
    return typeProvider;
  }

  public List<TSDataType> getOutputDataTypes() {
    return outputExpression.stream().map(expressionToDataType::get).collect(Collectors.toList());
  }

  public boolean isExpressionInput(Expression expression) {
    return inputLocations.containsKey(expression.getExpressionString());
  }
}
