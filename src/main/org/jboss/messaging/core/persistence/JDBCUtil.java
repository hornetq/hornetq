/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core.persistence;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a> 
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class JDBCUtil
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   public static String statementToString(String sql)
   {
      return sql;
   }

   public static String statementToString(String sql, Object arg1)
   {
      return statementToString(sql, new Object[] { arg1 });
   }

   public static String statementToString(String sql, Object arg1, Object arg2)
   {
      return statementToString(sql, new Object[] { arg1, arg2 });
   }

   public static String statementToString(String sql, Object arg1, Object arg2, Object arg3)
   {
      return statementToString(sql, new Object[] { arg1, arg2, arg3 });
   }

   public static String statementToString(String sql, Object arg1, Object arg2, Object arg3,
                                          Object arg4, Object arg5)
   {
      return statementToString(sql, new Object[] { arg1, arg2, arg3, arg4, arg5 });
   }

   public static String statementToString(String sql, Object[] args)
   {
      StringBuffer statement = new StringBuffer();
      int i = 0, pos = 0, cnt = 0;
      while((i = sql.indexOf('?', pos)) != -1)
      {
         statement.append(sql.substring(pos, i));
         Object s = "?";
         if (cnt < args.length)
         {
            s = args[cnt++];
         }
         statement.append(s);
         pos = i + 1;
      }
      statement.append(sql.substring(pos));
      return statement.toString();
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
