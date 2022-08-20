package org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.declares;

import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.DeclareStatement;

public class LongDeclareStatement extends DeclareStatement {

  public LongDeclareStatement(ExpressionNode es) {
    this.type = CodegenDataType.LONG;
    this.varName = es.getNodeName();
    this.es = es;
    generateNullCheck();
  }
}
