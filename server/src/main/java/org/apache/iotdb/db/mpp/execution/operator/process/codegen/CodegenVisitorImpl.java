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

import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.BetweenExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.BinaryExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ConstantExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.IdentityExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.IsNullExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.LeafExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.UnaryExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.AssignmentStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.DeclareStatement;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.InExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.NegationExpression;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFContext;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Objects;

public class CodegenVisitorImpl implements CodegenVisitor {

  private TimestampOperand globalTimestampOperand;

  private final CodegenContext codegenContext;

  private final UDTFContext udtfContext;

  private final TypeProvider typeProvider;

  public CodegenVisitorImpl(CodegenContext codegenContext) {
    this.typeProvider = codegenContext.getTypeProvider();
    this.udtfContext = codegenContext.getUdtfContext();
    this.codegenContext = codegenContext;
  }

  @Override
  public boolean expressionVisitor(Expression expression) {
    if (codegenContext.isExpressionExisted(expression)) {
      return true;
    }
    if (codegenContext.isExpressionInput(expression)) {
      String argName = codegenContext.uniqueVarName("input");
      LeafExpressionNode leafExpressionNode = new LeafExpressionNode(argName);
      codegenContext.addExpression(
          expression, leafExpressionNode, expression.inferTypes(typeProvider));
      codegenContext.addInputVarNameMap(expression.getExpressionString(), argName);
      codegenContext.addIntermediateVariable(
          createDeclareStatement(
              expression.inferTypes(typeProvider), new IdentityExpressionNode(argName)));
      return true;
    }
    return expression.codegenAccept(this);
  }

  @Override
  public boolean logicNotExpressionVisitor(LogicNotExpression logicNotExpression) {
    if (expressionVisitor(logicNotExpression.getExpression())) {
      ExpressionNode subNode = codegenContext.getExpressionNode(logicNotExpression.getExpression());
      UnaryExpressionNode notNode =
          new UnaryExpressionNode(codegenContext.uniqueVarName(), subNode, "!");
      codegenContext.addExpression(
          logicNotExpression, notNode, logicNotExpression.inferTypes(typeProvider));

      DeclareStatement boolDeclareStatement =
          new DeclareStatement("boolean", subNode.getNodeName());
      codegenContext.addIntermediateVariable(boolDeclareStatement);
      codegenContext.addAssignmentStatement(new AssignmentStatement(subNode));
      return true;
    }
    return false;
  }

  private DeclareStatement createDeclareStatement(
      TSDataType tsDataType, ExpressionNode expressionNode) {
    DeclareStatement statement;
    switch (tsDataType) {
      case INT32:
        statement = new DeclareStatement("int", expressionNode.getNodeName());
        break;
      case INT64:
        statement = new DeclareStatement("long", expressionNode.getNodeName());
        break;
      case FLOAT:
        statement = new DeclareStatement("float", expressionNode.getNodeName());
        break;
      case DOUBLE:
        statement = new DeclareStatement("double", expressionNode.getNodeName());
        break;
      case BOOLEAN:
        statement = new DeclareStatement("boolean", expressionNode.getNodeName());
        break;
        //      case TEXT:
        //        statement = new DeclareStatement("String",expressionNode.getNodeName(),
        // expressionNode);
        //        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported for expression codegen.", tsDataType));
    }
    return statement;
  }

  @Override
  public boolean negationExpressionVisitor(NegationExpression negationExpression) {
    if (expressionVisitor(negationExpression.getExpression())) {

      ExpressionNode subNode = codegenContext.getExpressionNode(negationExpression.getExpression());
      UnaryExpressionNode negationNode =
          new UnaryExpressionNode(codegenContext.uniqueVarName(), subNode, "-");

      TSDataType tsDataType = negationExpression.inferTypes(typeProvider);
      codegenContext.addExpression(negationExpression, negationNode, tsDataType);

      DeclareStatement statement;
      switch (tsDataType) {
        case INT32:
        case INT64:
        case FLOAT:
        case DOUBLE:
          statement = createDeclareStatement(tsDataType, negationNode);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format(
                  "Data type %s is not supported for negationExpression codegen.", tsDataType));
      }

      codegenContext.addIntermediateVariable(statement);
      codegenContext.addAssignmentStatement(new AssignmentStatement(negationNode));
      return true;
    }
    return false;
  }

  @Override
  public boolean binaryExpressionVisitor(BinaryExpression binaryExpression) {
    if (!expressionVisitor(binaryExpression.getRightExpression())) {
      return false;
    }
    if (!expressionVisitor(binaryExpression.getLeftExpression())) {
      return false;
    }

    ExpressionNode left = codegenContext.getExpressionNode(binaryExpression.getLeftExpression());
    String op = binaryExpression.getOperator();
    ExpressionNode right = codegenContext.getExpressionNode(binaryExpression.getRightExpression());

    BinaryExpressionNode binaryExpressionNode =
        new BinaryExpressionNode(codegenContext.uniqueVarName(), op, left, right);
    codegenContext.addExpression(
        binaryExpression, binaryExpressionNode, binaryExpression.inferTypes(typeProvider));

    DeclareStatement declareStatement =
        createDeclareStatement(binaryExpression.inferTypes(typeProvider), binaryExpressionNode);
    codegenContext.addIntermediateVariable(declareStatement);
    codegenContext.addAssignmentStatement(new AssignmentStatement(binaryExpressionNode));
    return true;
  }

  public boolean isNullExpressionVisitor(IsNullExpression isNullExpression) {
    Expression subExpression = isNullExpression.getExpression();
    if (!expressionVisitor(subExpression)) {
      return false;
    }
    ExpressionNode subExpressionNode = codegenContext.getExpressionNode(subExpression);

    IsNullExpressionNode isNullExpressionNode =
        new IsNullExpressionNode(
            codegenContext.uniqueVarName(), subExpressionNode, isNullExpression.isNot());

    codegenContext.addExpression(isNullExpression, isNullExpressionNode, TSDataType.BOOLEAN);
    codegenContext.addIntermediateVariable(
        new DeclareStatement("boolean", isNullExpressionNode.getNodeName()));
    codegenContext.addAssignmentStatement(new AssignmentStatement(isNullExpressionNode));
    return true;
  }

  @Override
  public boolean betweenExpressionVisitor(BetweenExpression betweenExpression) {
    if (!expressionVisitor(betweenExpression.getFirstExpression())) {
      return false;
    }
    if (!expressionVisitor(betweenExpression.getSecondExpression())) {
      return false;
    }
    if (!expressionVisitor(betweenExpression.getThirdExpression())) {
      return false;
    }

    boolean isNotBetween = betweenExpression.isNotBetween();

    ExpressionNode subExpressionNodeImpl =
        codegenContext.getExpressionNode(betweenExpression.getFirstExpression());
    ExpressionNode lowerNode =
        codegenContext.getExpressionNode(betweenExpression.getSecondExpression());
    ExpressionNode higherNode =
        codegenContext.getExpressionNode(betweenExpression.getThirdExpression());

    BetweenExpressionNode betweenExpressionNode =
        new BetweenExpressionNode(
            codegenContext.uniqueVarName(),
            subExpressionNodeImpl,
            lowerNode,
            higherNode,
            isNotBetween);

    codegenContext.addExpression(
        betweenExpression, betweenExpressionNode, betweenExpression.inferTypes(typeProvider));

    DeclareStatement declareStatement =
        createDeclareStatement(betweenExpression.inferTypes(typeProvider), betweenExpressionNode);
    codegenContext.addIntermediateVariable(declareStatement);
    codegenContext.addAssignmentStatement(new AssignmentStatement(betweenExpressionNode));
    return true;
  }

  @Override
  public boolean constantOperandVisitor(ConstantOperand constantOperand) {
    if (!codegenContext.isExpressionExisted(constantOperand)) {
      String valueString = constantOperand.getValueString();
      codegenContext.addExpression(
          constantOperand,
          new ConstantExpressionNode(valueString),
          constantOperand.inferTypes(typeProvider));
    }
    return true;
  }

  @Override
  // since timeseries always as input, this method should never be called
  public boolean timeSeriesOperandVisitor(TimeSeriesOperand timeSeriesOperand) {
    //    // don't support path like "root.sg.`a+b`
    //    String fullPath = timeSeriesOperand.getPath().getFullPath();
    //    String argName = fullPath.replace('.', '_');
    //
    //    LeafExpressionNode leafExpressionNode = new LeafExpressionNode(argName);
    //    codegenContext.addExpression(
    //        timeSeriesOperand, leafExpressionNode, timeSeriesOperand.inferTypes(typeProvider));

    return true;
  }

  @Override
  public boolean timestampOperandVisitor(TimestampOperand timestampOperand) {
    // To avoid repeat of TimestampOperand
    // all TimestampOperand will be replaced with globalTimestampOperand
    if (!codegenContext.isExpressionExisted(globalTimestampOperand)) {
      if (Objects.isNull(globalTimestampOperand)) {
        globalTimestampOperand = timestampOperand;
      }
      LeafExpressionNode timestamp = new LeafExpressionNode("timestamp");
      codegenContext.addExpression(globalTimestampOperand, timestamp, TSDataType.INT64);
    }
    return true;
  }

  public boolean inExpressionVisitor(InExpression inExpression) {
    //    if (!expressionVisitor(inExpression.getExpression())) {
    //      return false;
    //    }
    //    ExpressionNode subExpressionNode =
    //        codegenContext.getExpressionNode(inExpression.getExpression());
    //    String setName = codegenContext.uniqueVarName();
    //    NewSetStatement newSetStatement =
    //        new NewSetStatement(
    //            setName,
    //            new ArrayList<>(inExpression.getValues()),
    //            inExpression.getExpression().inferTypes(typeProvider));
    //    codegenContext.addCode(newSetStatement);
    //    InExpressionNode inExpressionNode =
    //        new InExpressionNode(
    //            codegenContext.uniqueVarName(), subExpressionNode, setName,
    // inExpression.isNotIn());
    //    BoolDeclareStatement boolDeclareStatement = new BoolDeclareStatement(inExpressionNode);
    //    codegenContext.addExpression(inExpression, inExpressionNode, TSDataType.BOOLEAN);
    //    codegenContext.addCode(boolDeclareStatement);
    return false;
  }

  @Override
  public boolean functionExpressionVisitor(FunctionExpression functionExpression) {
    return false;
    //    UDTFExecutor executor = udtfContext.getExecutorByFunctionExpression(functionExpression);
    //    if (executor.getConfigurations().getAccessStrategy().getAccessStrategyType()
    //        != MAPPABLE_ROW_BY_ROW) {
    //      return false;
    //    }
    //
    //    List<TSDataType> inputDatatype = new ArrayList<>();
    //    for (Expression expression : functionExpression.getExpressions()) {
    //      inputDatatype.add(expression.inferTypes(typeProvider));
    //      if (!expressionVisitor(expression)) {
    //        return false;
    //      }
    //    }
    //
    //    // get UDTFExecutor of udtf
    //    String executorName = codegenContext.uniqueVarName("executor");
    //    codegenContext.addUdtfExecutor(executorName, executor);
    //
    //    // generate a simpleRow of udtf
    //    CodegenSimpleRow inputRow = new CodegenSimpleRow(inputDatatype.toArray(new
    // TSDataType[0]));
    //    String rowName = codegenContext.uniqueVarName("row");
    //    codegenContext.addUdtfInput(rowName, inputRow);
    //
    //    FunctionExpressionNode functionExpressionNode =
    //        new FunctionExpressionNode(
    //            codegenContext.uniqueVarName(),
    //            executorName,
    //            rowName,
    //            functionExpression.inferTypes(typeProvider));
    //
    //    UpdateRowStatement updateRowStatement = new UpdateRowStatement(rowName);
    //    for (Expression expression : functionExpression.getExpressions()) {
    //      ExpressionNode subNode = codegenContext.getExpressionNode(expression);
    //      updateRowStatement.addData(subNode);
    //      functionExpressionNode.addSubExpressionNode(subNode);
    //    }
    //
    //    codegenContext.addCode(updateRowStatement);
    //
    //    // udtf may contain TimestampOperand
    //    // to avoid repeat of TimestampOperand, all TimestampOperand will be replaced with
    //    // globalTimestampOperand
    //    if (Objects.isNull(globalTimestampOperand)) {
    //      globalTimestampOperand = new TimestampOperand();
    //    }
    //
    //    if (!codegenContext.isExpressionExisted(globalTimestampOperand)) {
    //      LeafExpressionNode timestamp = new LeafExpressionNode("timestamp");
    //      codegenContext.addExpression(globalTimestampOperand, timestamp, TSDataType.INT64);
    //    }
    //
    //    codegenContext.addExpression(
    //        functionExpression, functionExpressionNode,
    // functionExpression.inferTypes(typeProvider));
    //    Statement declareStatement =
    //        createDeclareStatement(functionExpression.inferTypes(typeProvider),
    // functionExpressionNode);
    //    codegenContext.addCode(declareStatement);
    //    return true;
  }
}
