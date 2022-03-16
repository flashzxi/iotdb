package org.apache.iotdb.db.query.udf.core.transformer;

import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;

public class LogicAndTransformer extends CompareOperatorTransformer {

  public LogicAndTransformer(LayerPointReader leftPointReader, LayerPointReader rightPointReader) {
    super(leftPointReader, rightPointReader);
  }

  @Override
  protected double evaluate(double leftOperand, double rightOperand) {
    return (leftOperand == 1.0) && (rightOperand == 1.0) ? 1.0 : 0.0;
  }
}
