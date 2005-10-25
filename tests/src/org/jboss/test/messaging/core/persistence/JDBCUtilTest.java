/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core.persistence;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.core.persistence.JDBCUtil;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class JDBCUtilTest extends MessagingTestCase
{
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public JDBCUtilTest(String name)
   {
      super(name);
   }

   public void testStatementToStringOneArgument()
   {
      String sql = "INSERT INTO A (B) values(?)";
      String statement = JDBCUtil.statementToString(sql, "X");
      assertEquals("INSERT INTO A (B) values(X)", statement);
   }

   public void testStatementToStringTwoArguments()
   {
      String sql = "INSERT INTO A (B, C) values(?, ?)";
      String statement = JDBCUtil.statementToString(sql, "X", "Y");
      assertEquals("INSERT INTO A (B, C) values(X, Y)", statement);
   }

   public void testStatementToStringWitNull()
   {
      String sql = "INSERT INTO A (B, C) values(?, ?)";
      String statement = JDBCUtil.statementToString(sql, null, "Y");
      assertEquals("INSERT INTO A (B, C) values(null, Y)", statement);
   }


   public void testExtraArguments()
   {
      String sql = "INSERT INTO A (B, C) values(?, ?)";
      String statement = JDBCUtil.statementToString(sql, "X", "Y", "Z");
      assertEquals("INSERT INTO A (B, C) values(X, Y)", statement);
   }

   public void testNotEnoughArguments()
   {
      String sql = "INSERT INTO A (B, C, D) values(?, ?, ?)";
      String statement = JDBCUtil.statementToString(sql, "X", "Y");
      assertEquals("INSERT INTO A (B, C, D) values(X, Y, ?)", statement);
   }




}


