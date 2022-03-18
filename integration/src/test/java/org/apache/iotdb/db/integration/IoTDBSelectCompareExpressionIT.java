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

package org.apache.iotdb.db.integration;

import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

@Category({LocalStandaloneTest.class, ClusterTest.class})
public class IoTDBSelectCompareExpressionIT {

  private static String[] INSERTION_SQLS;
  private static List<Long> time = new ArrayList<>(0);
  private static List<Integer> _int = new ArrayList<>(0);
  private static List<Long> _long = new ArrayList<>(0);
  private static List<Float> _float = new ArrayList<>(0);
  private static List<Double> _double = new ArrayList<>(0);
  private static List<Boolean> _bool = new ArrayList<>(0);

  private static void generateInsertionSQLS() {
    INSERTION_SQLS = new String[50];
    Random random = new Random();
    for (int j = 0; j < 50; ++j) {
      _int.add(random.nextInt(10));
      _long.add((long) random.nextInt(10));
      _float.add((float) (random.nextInt(100) / 10.0));
      _double.add(random.nextInt(100) / 10.0);
      _bool.add(random.nextBoolean());
      INSERTION_SQLS[j] =
          generateInsertionSQL(
              (long) j,
              _int.get(_int.size() - 1),
              _long.get(_long.size() - 1),
              _float.get(_float.size() - 1),
              _double.get(_double.size() - 1),
              _bool.get(_bool.size() - 1),
              "\"magic_words\"");
    }
  }

  private static String generateInsertionSQL(
      long time,
      int _int32,
      long _int64,
      float _float,
      double _double,
      boolean _bool,
      String _text) {
    return String.format(
        "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6) values (%d, %d, %d, %f, %f, %s, %s);",
        time, _int32, _int64, _float, _double, _bool ? "true" : "false", _text);
  }

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
    createTimeSeries();
    generateData();
  }

  private static void generateData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      generateInsertionSQLS();
      for (String dataGenerationSql : INSERTION_SQLS) {
        statement.execute(dataGenerationSql);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.sg");
      statement.execute("CREATE TIMESERIES root.sg.d1.s1 with datatype=INT32,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.s2 with datatype=INT64,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.s3 with datatype=FLOAT,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.s4 with datatype=DOUBLE,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.s5 with datatype=BOOLEAN,encoding=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg.d1.s6 with datatype=TEXT,encoding=PLAIN");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
  }

  /*
   * Test compare expressions between different TSDataType
   * */
  @Test
  public void testCompareWithConstant() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery("select s1>=5, s1<=5, s1>5, s1<5, s1==5, s1!=5 from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(_int.get(i) >= 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(3));
        assertEquals(_int.get(i) <= 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(4));
        assertEquals(_int.get(i) > 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(5));
        assertEquals(_int.get(i) < 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(6));
        assertEquals(_int.get(i) == 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(7));
        assertEquals(_int.get(i) != 5, bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery("select s2>=5, s2<=5, s2>5, s2<5, s2==5, s2!=5 from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(_long.get(i) >= 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(3));
        assertEquals(_long.get(i) <= 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(4));
        assertEquals(_long.get(i) > 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(5));
        assertEquals(_long.get(i) < 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(6));
        assertEquals(_long.get(i) == 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(7));
        assertEquals(_long.get(i) != 5, bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery("select s3>=5, s3<=5, s3>5, s3<5, s3==5, s3!=5 from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(_float.get(i) >= 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(3));
        assertEquals(_float.get(i) <= 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(4));
        assertEquals(_float.get(i) > 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(5));
        assertEquals(_float.get(i) < 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(6));
        assertEquals(_float.get(i) == 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(7));
        assertEquals(_float.get(i) != 5, bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery("select s4>=5, s4<=5, s4>5, s4<5, s4==5, s4!=5 from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(_double.get(i) >= 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(3));
        assertEquals(_double.get(i) <= 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(4));
        assertEquals(_double.get(i) > 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(5));
        assertEquals(_double.get(i) < 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(6));
        assertEquals(_double.get(i) == 5, bool);

        bool = Boolean.parseBoolean(resultSet.getString(7));
        assertEquals(_double.get(i) != 5, bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery("select s5==true, s5!=true, s5==false, s5!=false from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 4, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(_bool.get(i) == true, bool);

        bool = Boolean.parseBoolean(resultSet.getString(3));
        assertEquals(_bool.get(i) != true, bool);

        bool = Boolean.parseBoolean(resultSet.getString(4));
        assertEquals(_bool.get(i) == false, bool);

        bool = Boolean.parseBoolean(resultSet.getString(5));
        assertEquals(_bool.get(i) != false, bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testCompareDifferentType() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery(
              "select s1>=s2, s1<=s2, s1>s3, s1<s3, s1==s4, s1!=s4 from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(_int.get(i) >= _long.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(3));
        assertEquals(_int.get(i) <= _long.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(4));
        assertEquals(_int.get(i) > _float.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(5));
        assertEquals(_int.get(i) < _float.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(6));
        assertEquals((double) _int.get(i) == _double.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(7));
        assertEquals((double) _int.get(i) != _double.get(i), bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery(
              "select s2>=s3, s2<=s3, s2>s4, s2<s4, s2==s1, s2!=s1 from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(_long.get(i) >= _float.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(3));
        assertEquals(_long.get(i) <= _float.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(4));
        assertEquals(_long.get(i) > _double.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(5));
        assertEquals(_long.get(i) < _double.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(6));
        assertEquals(_long.get(i) == (long) _int.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(7));
        assertEquals(_long.get(i) != (long) _int.get(i), bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery(
              "select s3>=s4, s3<=s4, s3>s1, s3<s1, s3==s2, s3!=s2 from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(_float.get(i) >= _double.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(3));
        assertEquals(_float.get(i) <= _double.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(4));
        assertEquals(_float.get(i) > _int.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(5));
        assertEquals(_float.get(i) < _int.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(6));
        assertEquals(_float.get(i) == (float) _long.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(7));
        assertEquals(_float.get(i) != (float) _long.get(i), bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery(
              "select s4>=s1, s4<=s1, s4>s2, s4<s2, s4==s3, s4!=s3 from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(_double.get(i) >= _int.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(3));
        assertEquals(_double.get(i) <= _int.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(4));
        assertEquals(_double.get(i) > _long.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(5));
        assertEquals(_double.get(i) < _long.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(6));
        assertEquals(_double.get(i) == (double) _float.get(i), bool);

        bool = Boolean.parseBoolean(resultSet.getString(7));
        assertEquals(_double.get(i) != (double) _float.get(i), bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testLogicOrAndNot() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet =
          statement.executeQuery("select s1>=1 && s1<3, !(s1 < 2 || s1> 8), !s2>3 from root.sg.d1");
      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 3, columnCount);

      boolean bool;
      for (int i = 0; i < time.size(); ++i) {
        resultSet.next();

        bool = Boolean.parseBoolean(resultSet.getString(2));
        assertEquals(_int.get(i) >= 1 && _int.get(i) < 3, bool);

        bool = Boolean.parseBoolean(resultSet.getString(3));
        assertEquals(!(_int.get(i) < 2 || _int.get(i) > 8), bool);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
