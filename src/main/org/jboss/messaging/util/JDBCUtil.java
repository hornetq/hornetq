/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.messaging.util;

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
                                          Object arg4)
   {
      return statementToString(sql, new Object[] { arg1, arg2, arg3, arg4 });
   }

   public static String statementToString(String sql, Object arg1, Object arg2, Object arg3,
                                          Object arg4, Object arg5)
   {
      return statementToString(sql, new Object[] { arg1, arg2, arg3, arg4, arg5 });
   }
   
   public static String statementToString(String sql, Object arg1, Object arg2, Object arg3,
         Object arg4, Object arg5, Object arg6)
   {
      return statementToString(sql, new Object[] { arg1, arg2, arg3, arg4, arg5, arg6 });
   }
   
   public static String statementToString(String sql, Object arg1, Object arg2, Object arg3,
         Object arg4, Object arg5, Object arg6, Object arg7)
   {
      return statementToString(sql, new Object[] { arg1, arg2, arg3, arg4, arg5, arg6, arg7 });
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
            if ("".equals(s))
            {
               s = "''";
            }
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
