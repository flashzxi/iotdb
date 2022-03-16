package org.apache.iotdb.db.query.expression.binary;

import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.db.query.udf.core.transformer.ArithmeticBinaryTransformer;
import org.apache.iotdb.db.query.udf.core.transformer.CompareGreaterThanTransformer;

public class GreaterThanExpression extends BinaryExpression {
  public GreaterThanExpression(Expression leftExpression, Expression rightExpression) {
    super(leftExpression, rightExpression);
  }

  @Override
  protected ArithmeticBinaryTransformer constructTransformer(
      LayerPointReader leftParentLayerPointReader, LayerPointReader rightParentLayerPointReader) {
    return new CompareGreaterThanTransformer(
        leftParentLayerPointReader, rightParentLayerPointReader);
  }

  @Override
  protected String operator() {
    return ">";
  }
}
