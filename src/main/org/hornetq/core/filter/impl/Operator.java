/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.hornetq.core.filter.impl;

import java.util.HashSet;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;

/**
* Implementations of the operators used in HornetQ filter expressions
*
* @author Norbert Lataille (Norbert.Lataille@m4x.org)
* @author droy@boostmyscore.com
* @author Scott.Stark@jboss.org
* @author adrian@jboss.com
* @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
* @version $Revision: 2681 $
*/
public class Operator
{
   private static final Logger log = Logger.getLogger(Operator.class);

   int operation;

   Object oper1;

   Object oper2;

   Object oper3;

   Object arg1;

   Object arg2;

   Object arg3;

   int class1;

   int class2;

   int class3;

   // info about the regular expression
   // if this is a LIKE operator
   // (perhaps this should be a subclass)
   RegExp re = null;

   public final static int EQUAL = 0;

   public final static int NOT = 1;

   public final static int AND = 2;

   public final static int OR = 3;

   public final static int GT = 4;

   public final static int GE = 5;

   public final static int LT = 6;

   public final static int LE = 7;

   public final static int DIFFERENT = 8;

   public final static int ADD = 9;

   public final static int SUB = 10;

   public final static int NEG = 11;

   public final static int MUL = 12;

   public final static int DIV = 13;

   public final static int BETWEEN = 14;

   public final static int NOT_BETWEEN = 15;

   public final static int LIKE = 16;

   public final static int NOT_LIKE = 17;

   public final static int LIKE_ESCAPE = 18;

   public final static int NOT_LIKE_ESCAPE = 19;

   public final static int IS_NULL = 20;

   public final static int IS_NOT_NULL = 21;

   public final static int IN = 22;

   public final static int NOT_IN = 23;

   public final static int DOUBLE = 1;

   // DOUBLE FLOAT
   public final static int LONG = 2;

   // LONG BYTE SHORT INTEGER
   public final static int BOOLEAN = 3;

   public final static int SIMPLE_STRING = 4;

   public Operator(final int operation, final Object oper1, final Object oper2, final Object oper3)
   {
      this.operation = operation;
      this.oper1 = oper1;
      this.oper2 = oper2;
      this.oper3 = oper3;
   }

   public Operator(final int operation, final Object oper1, final Object oper2)
   {
      this.operation = operation;
      this.oper1 = oper1;
      this.oper2 = oper2;
      oper3 = null;
   }

   public Operator(final int operation, final Object oper1)
   {
      this.operation = operation;
      this.oper1 = oper1;
      oper2 = null;
      oper3 = null;
   }

   // --- Print functions ---

   @Override
   public String toString()
   {
      return print("");
   }

   public String print(final String level)
   {
      String st = level + operation + ":" + Operator.operationString(operation) + "(\n";

      String nextLevel = level + "  ";

      if (oper1 == null)
      {
         st += nextLevel + "null\n";
      }
      else if (oper1 instanceof Operator)
      {
         st += ((Operator)oper1).print(nextLevel);
      }
      else
      {
         st += nextLevel + oper1.toString() + "\n";
      }

      if (oper2 != null)
      {
         if (oper2 instanceof Operator)
         {
            st += ((Operator)oper2).print(nextLevel);
         }
         else
         {
            st += nextLevel + oper2.toString() + "\n";
         }
      }

      if (oper3 != null)
      {
         if (oper3 instanceof Operator)
         {
            st += ((Operator)oper3).print(nextLevel);
         }
         else
         {
            st += nextLevel + oper3.toString() + "\n";
         }
      }

      st += level + ")\n";

      return st;
   }

   // Operator 20
   Object is_null() throws Exception
   {
      computeArgument1();
      if (arg1 == null)
      {
         return Boolean.TRUE;
      }
      else
      {
         return Boolean.FALSE;
      }
   }

   // Operator 21
   Object is_not_null() throws Exception
   {
      computeArgument1();
      if (arg1 != null)
      {
         return Boolean.TRUE;
      }
      else
      {
         return Boolean.FALSE;
      }
   }

   // Operation 0
   Object equal() throws Exception
   {
      computeArgument1();
      if (arg1 == null)
      {
         return Boolean.FALSE;
      }

      switch (class1)
      {
         case LONG:
            computeArgument1();
            if (arg1 == null)
            {
               return null;
            }
            computeArgument2();
            if (arg2 == null)
            {
               return null;
            }
            if (class2 == Operator.LONG)
            {
               return Boolean.valueOf(((Number)arg1).longValue() == ((Number)arg2).longValue());
            }
            if (class2 == Operator.DOUBLE)
            {
               return Boolean.valueOf(((Number)arg1).longValue() == ((Number)arg2).doubleValue());
            }
            return Boolean.FALSE;
         case DOUBLE:
            computeArgument1();
            if (arg1 == null)
            {
               return null;
            }
            computeArgument2();
            if (arg2 == null)
            {
               return null;
            }
            if (class2 == Operator.LONG)
            {
               return Boolean.valueOf(((Number)arg1).doubleValue() == ((Number)arg2).longValue());
            }
            if (class2 == Operator.DOUBLE)
            {
               return Boolean.valueOf(((Number)arg1).doubleValue() == ((Number)arg2).doubleValue());
            }
            return Boolean.FALSE;
         case SIMPLE_STRING:
         case BOOLEAN:
            computeArgument2();
            if (arg2 == null)
            {
               return Boolean.FALSE;
            }
            if (class2 != class1)
            {
               throwBadObjectException(class1, class2);
            }
            return Boolean.valueOf(arg1.equals(arg2));
         default:
            throwBadObjectException(class1);
            return null;
      }

   }

   // Operation 1
   Object not() throws Exception
   {
      computeArgument1();
      if (arg1 == null)
      {
         return null;
      }
      if (class1 != Operator.BOOLEAN)
      {
         throwBadObjectException(class1);
      }
      if (((Boolean)arg1).booleanValue())
      {
         return Boolean.FALSE;
      }
      else
      {
         return Boolean.TRUE;
      }
   }

   // Operation 2
   Object and() throws Exception
   {
      computeArgument1();
      if (arg1 == null)
      {
         computeArgument2();
         if (arg2 == null)
         {
            return null;
         }
         if (class2 != Operator.BOOLEAN)
         {
            throwBadObjectException(class2);
         }
         if (((Boolean)arg2).booleanValue() == false)
         {
            return Boolean.FALSE;
         }
         return null;
      }

      if (class1 == Operator.BOOLEAN)
      {
         if (((Boolean)arg1).booleanValue() == false)
         {
            return Boolean.FALSE;
         }
         computeArgument2();
         if (arg2 == null)
         {
            return null;
         }
         if (class2 != Operator.BOOLEAN)
         {
            throwBadObjectException(class2);
         }
         return arg2;
      }

      throwBadObjectException(class1);
      return null;
   }

   /**
    * Operation 3
    * 
    * | OR   |   T   |   F   |   U
    * +------+-------+-------+--------
    * |  T   |   T   |   T   |   T
    * |  F   |   T   |   F   |   U
    * |  U   |   T   |   U   |   U
    * +------+-------+-------+------- 
    */
   Object or() throws Exception
   {
      short falseCounter = 0;

      computeArgument1();
      if (arg1 != null)
      {
         if (class1 != Operator.BOOLEAN)
         {
            throwBadObjectException(class1);
         }
         if (((Boolean)arg1).booleanValue())
         {
            return Boolean.TRUE;
         }
         else
         {
            falseCounter++;
         }
      }

      computeArgument2();
      if (arg2 != null)
      {
         if (class2 != Operator.BOOLEAN)
         {
            throwBadObjectException(class2);
         }
         if (((Boolean)arg2).booleanValue())
         {
            return Boolean.TRUE;
         }
         else
         {
            falseCounter++;
         }
      }

      if (falseCounter == 2)
      {
         return Boolean.FALSE;
      }

      return null;
   }

   // Operation 4
   Object gt() throws Exception
   {
      computeArgument1();
      if (arg1 == null)
      {
         return null;
      }

      if (class1 == Operator.LONG)
      {
         computeArgument2();
         if (arg2 == null)
         {
            return null;
         }
         if (class2 == Operator.LONG)
         {
            return Boolean.valueOf(((Number)arg1).longValue() > ((Number)arg2).longValue());
         }
         if (class2 == Operator.DOUBLE)
         {
            return Boolean.valueOf(((Number)arg1).longValue() > ((Number)arg2).doubleValue());
         }
      }
      else if (class1 == Operator.DOUBLE)
      {
         computeArgument2();
         if (arg2 == null)
         {
            return null;
         }
         if (class2 == Operator.LONG)
         {
            return Boolean.valueOf(((Number)arg1).doubleValue() > ((Number)arg2).longValue());
         }
         if (class2 == Operator.DOUBLE)
         {
            return Boolean.valueOf(((Number)arg1).doubleValue() > ((Number)arg2).doubleValue());
         }
         return Boolean.FALSE;
      }
      return Boolean.FALSE;
   }

   // Operation 5
   Object ge() throws Exception
   {
      computeArgument1();
      if (arg1 == null)
      {
         return null;
      }

      if (class1 == Operator.LONG)
      {
         computeArgument2();
         if (arg2 == null)
         {
            return null;
         }
         if (class2 == Operator.LONG)
         {
            return Boolean.valueOf(((Number)arg1).longValue() >= ((Number)arg2).longValue());
         }
         if (class2 == Operator.DOUBLE)
         {
            return Boolean.valueOf(((Number)arg1).longValue() >= ((Number)arg2).doubleValue());
         }
      }
      else if (class1 == Operator.DOUBLE)
      {
         computeArgument2();
         if (arg2 == null)
         {
            return null;
         }
         if (class2 == Operator.LONG)
         {
            return Boolean.valueOf(((Number)arg1).longValue() >= ((Number)arg2).longValue());
         }
         if (class2 == Operator.DOUBLE)
         {
            return Boolean.valueOf(((Number)arg1).doubleValue() >= ((Number)arg2).doubleValue());
         }
         return Boolean.FALSE;
      }
      return Boolean.FALSE;
   }

   // Operation 6
   Object lt() throws Exception
   {
      computeArgument1();
      if (arg1 == null)
      {
         return null;
      }

      if (class1 == Operator.LONG)
      {
         computeArgument2();
         if (arg2 == null)
         {
            return null;
         }
         if (class2 == Operator.LONG)
         {
            return Boolean.valueOf(((Number)arg1).longValue() < ((Number)arg2).longValue());
         }
         if (class2 == Operator.DOUBLE)
         {
            return Boolean.valueOf(((Number)arg1).longValue() < ((Number)arg2).doubleValue());
         }
      }
      else if (class1 == Operator.DOUBLE)
      {
         computeArgument2();
         if (arg2 == null)
         {
            return null;
         }
         if (class2 == Operator.LONG)
         {
            return Boolean.valueOf(((Number)arg1).doubleValue() < ((Number)arg2).longValue());
         }
         if (class2 == Operator.DOUBLE)
         {
            return Boolean.valueOf(((Number)arg1).doubleValue() < ((Number)arg2).doubleValue());
         }
      }

      return Boolean.FALSE;
   }

   // Operation 7
   Object le() throws Exception
   {
      computeArgument1();
      if (arg1 == null)
      {
         return null;
      }

      if (class1 == Operator.LONG)
      {
         computeArgument2();
         if (arg2 == null)
         {
            return null;
         }
         if (class2 == Operator.LONG)
         {
            return Boolean.valueOf(((Number)arg1).longValue() <= ((Number)arg2).longValue());
         }
         if (class2 == Operator.DOUBLE)
         {
            return Boolean.valueOf(((Number)arg1).longValue() <= ((Number)arg2).doubleValue());
         }
      }
      else if (class1 == Operator.DOUBLE)
      {
         computeArgument2();
         if (arg2 == null)
         {
            return null;
         }
         if (class2 == Operator.LONG)
         {
            return Boolean.valueOf(((Number)arg1).doubleValue() <= ((Number)arg2).longValue());
         }
         if (class2 == Operator.DOUBLE)
         {
            return Boolean.valueOf(((Number)arg1).doubleValue() <= ((Number)arg2).doubleValue());
         }
      }
      return Boolean.FALSE;
   }

   // Operation 8
   Object different() throws Exception
   {
      computeArgument1();

      if (arg1 == null)
      {
         computeArgument2();
         if (arg2 == null)
         {
            return Boolean.FALSE;
         }
         else
         {
            return Boolean.TRUE;
         }
      }

      switch (class1)
      {
         case LONG:
            computeArgument1();
            if (arg1 == null)
            {
               return null;
            }
            computeArgument2();
            if (arg2 == null)
            {
               return null;
            }
            if (class2 == Operator.LONG)
            {
               return Boolean.valueOf(((Number)arg1).longValue() != ((Number)arg2).longValue());
            }
            if (class2 == Operator.DOUBLE)
            {
               return Boolean.valueOf(((Number)arg1).longValue() != ((Number)arg2).doubleValue());
            }
            return Boolean.FALSE;
         case DOUBLE:
            computeArgument1();
            if (arg1 == null)
            {
               return null;
            }
            computeArgument2();
            if (arg2 == null)
            {
               return null;
            }
            if (class2 == Operator.LONG)
            {
               return Boolean.valueOf(((Number)arg1).doubleValue() != ((Number)arg2).longValue());
            }
            if (class2 == Operator.DOUBLE)
            {
               return Boolean.valueOf(((Number)arg1).doubleValue() != ((Number)arg2).doubleValue());
            }
            return Boolean.FALSE;
         case SIMPLE_STRING:
         case BOOLEAN:
            computeArgument2();
            if (arg2 == null)
            {
               return null;
            }
            if (class2 != class1)
            {
               throwBadObjectException(class1, class2);
            }
            return Boolean.valueOf(arg1.equals(arg2) == false);
         default:
            throwBadObjectException(class1);
      }
      return null;
   }

   // Operator 9
   Object add() throws Exception
   {
      computeArgument1();
      computeArgument2();

      if (arg1 == null || arg2 == null)
      {
         return null;
      }
      switch (class1)
      {
         case DOUBLE:
            switch (class2)
            {
               case DOUBLE:
                  return new Double(((Number)arg1).doubleValue() + ((Number)arg2).doubleValue());
               case LONG:
                  return new Double(((Number)arg1).doubleValue() + ((Number)arg2).doubleValue());
               default:
                  throwBadObjectException(class2);
            }
         case LONG:
            switch (class2)
            {
               case DOUBLE:
                  return new Double(((Number)arg1).doubleValue() + ((Number)arg2).doubleValue());
               case LONG:
                  return new Long(((Number)arg1).longValue() + ((Number)arg2).longValue());
               default:
                  throwBadObjectException(class2);
            }
         default:
            throwBadObjectException(class1);
      }
      return null;
   }

   // Operator 10
   Object sub() throws Exception
   {
      computeArgument1();
      computeArgument2();

      if (arg1 == null || arg2 == null)
      {
         return null;
      }
      switch (class1)
      {
         case DOUBLE:
            switch (class2)
            {
               case DOUBLE:
                  return new Double(((Number)arg1).doubleValue() - ((Number)arg2).doubleValue());
               case LONG:
                  return new Double(((Number)arg1).doubleValue() - ((Number)arg2).doubleValue());
               default:
                  throwBadObjectException(class2);
            }
         case LONG:
            switch (class2)
            {
               case DOUBLE:
                  return new Double(((Number)arg1).doubleValue() - ((Number)arg2).doubleValue());
               case LONG:
                  return new Long(((Number)arg1).longValue() - ((Number)arg2).longValue());
               default:
                  throwBadObjectException(class2);
            }
         default:
            throwBadObjectException(class1);
      }
      return null;
   }

   // Operator 11
   Object neg() throws Exception
   {
      computeArgument1();
      if (arg1 == null)
      {
         return null;
      }
      switch (class1)
      {
         case DOUBLE:
            return new Double(-((Number)arg1).doubleValue());
         case LONG:
            return new Long(-((Number)arg1).longValue());
         default:
            throwBadObjectException(class1);
      }
      return null;
   }

   // Operator 12
   Object mul() throws Exception
   {
      computeArgument1();
      computeArgument2();
      if (arg1 == null || arg2 == null)
      {
         return null;
      }
      switch (class1)
      {
         case DOUBLE:
            switch (class2)
            {
               case DOUBLE:
                  return new Double(((Number)arg1).doubleValue() * ((Number)arg2).doubleValue());
               case LONG:
                  return new Double(((Number)arg1).doubleValue() * ((Number)arg2).doubleValue());
               default:
                  throwBadObjectException(class2);
            }
         case LONG:
            switch (class2)
            {
               case DOUBLE:
                  return new Double(((Number)arg1).doubleValue() * ((Number)arg2).doubleValue());
               case LONG:
                  return new Long(((Number)arg1).longValue() * ((Number)arg2).longValue());
               default:
                  throwBadObjectException(class2);
            }
         default:
            throwBadObjectException(class1);
      }
      return null;
   }

   // Operator 13
   Object div() throws Exception
   {
      // Can throw Divide by zero exception...
      computeArgument1();
      computeArgument2();
      if (arg1 == null || arg2 == null)
      {
         return null;
      }
      switch (class1)
      {
         case DOUBLE:
            switch (class2)
            {
               case DOUBLE:
                  return new Double(((Number)arg1).doubleValue() / ((Number)arg2).doubleValue());
               case LONG:
                  return new Double(((Number)arg1).doubleValue() / ((Number)arg2).doubleValue());
               default:
                  throwBadObjectException(class2);
            }
         case LONG:
            switch (class2)
            {
               case DOUBLE:
                  return new Double(((Number)arg1).doubleValue() / ((Number)arg2).doubleValue());
               case LONG:
                  return new Long(((Number)arg1).longValue() / ((Number)arg2).longValue());
               default:
                  throwBadObjectException(class2);
            }
         default:
            throwBadObjectException(class1);
      }
      return null;
   }

   // Operator 14
   Object between() throws Exception
   {
      Object res = ge();
      if (res == null)
      {
         return null;
      }
      if (((Boolean)res).booleanValue() == false)
      {
         return res;
      }

      Object oper4 = oper2;
      oper2 = oper3;
      res = le();
      oper2 = oper4;
      return res;
   }

   // Operator 15
   Object not_between() throws Exception
   {
      Object res = lt();
      if (res == null)
      {
         return null;
      }
      if (((Boolean)res).booleanValue())
      {
         return res;
      }

      Object oper4 = oper2;
      oper2 = oper3;
      res = gt();
      oper2 = oper4;
      return res;
   }

   // Operation 16,17,18,19
   /**
    *  Handle LIKE, NOT LIKE, LIKE ESCAPE, and NOT LIKE ESCAPE operators.
    *
    * @param  not            true if this is a NOT LIKE construct, false if this
    *      is a LIKE construct.
    * @param  use_escape     true if this is a LIKE ESCAPE construct, false if
    *      there is no ESCAPE clause
    * @return                Description of the Returned Value
    * @exception  Exception  Description of Exception
    */
   Object like(final boolean not, final boolean use_escape) throws Exception
   {
      Character escapeChar = null;

      computeArgument1();

      if (arg1 == null)
      {
         return null;
      }

      if (class1 != Operator.SIMPLE_STRING)
      {
         throwBadObjectException(class1);
      }

      computeArgument2();

      if (arg2 == null)
      {
         return Boolean.FALSE;
      }
      if (class2 != Operator.SIMPLE_STRING)
      {
         throwBadObjectException(class2);
      }

      if (use_escape)
      {
         computeArgument3();
         if (arg3 == null)
         {
            return null;
         }

         if (class3 != Operator.SIMPLE_STRING)
         {
            throwBadObjectException(class3);
         }

         SimpleString escapeString = (SimpleString)arg3;
         if (escapeString.length() != 1)
         {
            throw new Exception("LIKE ESCAPE: Bad escape character " + escapeString.toString());
         }

         escapeChar = new Character(escapeString.charAt(0));
      }

      if (re == null)
      {
         // the first time through we prepare the regular expression
         re = new RegExp(arg2.toString(), escapeChar);
      }

      boolean result = re.isMatch(arg1);
      if (not)
      {
         result = !result;
      }

      if (result == true)
      {
         return Boolean.TRUE;
      }
      else
      {
         return Boolean.FALSE;
      }
   }

   // Operator 22
   Object in() throws Exception
   {
      computeArgument1();
      if (arg1 == null)
      {
         return false;
      }
      if (class1 != Operator.SIMPLE_STRING)
      {
         throwBadObjectException(class1);
      }
      if (((HashSet)oper2).contains(arg1))
      {
         return Boolean.TRUE;
      }
      else
      {
         return Boolean.FALSE;
      }
   }

   // Operator 23
   Object not_in() throws Exception
   {
      computeArgument1();
      if (arg1 == null)
      {
         return null;
      }
      if (class1 != Operator.SIMPLE_STRING)
      {
         throwBadObjectException(class1);
      }
      if (((HashSet)oper2).contains(arg1))
      {
         return Boolean.FALSE;
      }
      else
      {
         return Boolean.TRUE;
      }
   }

   void computeArgument1() throws Exception
   {
      if (oper1 == null)
      {
         class1 = 0;
         return;
      }
      Class className = oper1.getClass();

      if (className == Identifier.class)
      {
         arg1 = ((Identifier)oper1).getValue();
      }
      else if (className == Operator.class)
      {
         arg1 = ((Operator)oper1).apply();
      }
      else
      {
         arg1 = oper1;
      }

      if (arg1 == null)
      {
         class1 = 0;
         return;
      }

      className = arg1.getClass();

      if (className == SimpleString.class)
      {
         class1 = Operator.SIMPLE_STRING;
      }
      else if (className == Double.class)
      {
         class1 = Operator.DOUBLE;
      }
      else if (className == Long.class)
      {
         class1 = Operator.LONG;
      }
      else if (className == Integer.class)
      {
         class1 = Operator.LONG;
         arg1 = new Long(((Integer)arg1).longValue());
      }
      else if (className == Short.class)
      {
         class1 = Operator.LONG;
         arg1 = new Long(((Short)arg1).longValue());
      }
      else if (className == Byte.class)
      {
         class1 = Operator.LONG;
         arg1 = new Long(((Byte)arg1).longValue());
      }
      else if (className == Float.class)
      {
         class1 = Operator.DOUBLE;
         arg1 = new Double(((Float)arg1).doubleValue());
      }
      else if (className == Boolean.class)
      {
         class1 = Operator.BOOLEAN;
      }
      else
      {
         throwBadObjectException(className);
      }
   }

   void computeArgument2() throws Exception
   {
      if (oper2 == null)
      {
         class2 = 0;
         return;
      }

      Class className = oper2.getClass();

      if (className == Identifier.class)
      {
         arg2 = ((Identifier)oper2).getValue();
      }
      else if (className == Operator.class)
      {
         arg2 = ((Operator)oper2).apply();
      }
      else
      {
         arg2 = oper2;
      }

      if (arg2 == null)
      {
         class2 = 0;
         return;
      }

      className = arg2.getClass();

      if (className == SimpleString.class)
      {
         class2 = Operator.SIMPLE_STRING;
      }
      else if (className == Double.class)
      {
         class2 = Operator.DOUBLE;
      }
      else if (className == Long.class)
      {
         class2 = Operator.LONG;
      }
      else if (className == Integer.class)
      {
         class2 = Operator.LONG;
         arg2 = new Long(((Integer)arg2).longValue());
      }
      else if (className == Short.class)
      {
         class2 = Operator.LONG;
         arg2 = new Long(((Short)arg2).longValue());
      }
      else if (className == Byte.class)
      {
         class2 = Operator.LONG;
         arg2 = new Long(((Byte)arg2).longValue());
      }
      else if (className == Float.class)
      {
         class2 = Operator.DOUBLE;
         arg2 = new Double(((Float)arg2).doubleValue());
      }
      else if (className == Boolean.class)
      {
         class2 = Operator.BOOLEAN;
      }
      else
      {
         throwBadObjectException(className);
      }
   }

   void computeArgument3() throws Exception
   {
      if (oper3 == null)
      {
         class3 = 0;
         return;
      }

      Class className = oper3.getClass();

      if (className == Identifier.class)
      {
         arg3 = ((Identifier)oper3).getValue();
      }
      else if (className == Operator.class)
      {
         arg3 = ((Operator)oper3).apply();
      }
      else
      {
         arg3 = oper3;
      }

      if (arg3 == null)
      {
         class3 = 0;
         return;
      }

      className = arg3.getClass();

      if (className == SimpleString.class)
      {
         class3 = Operator.SIMPLE_STRING;
      }
      else if (className == Double.class)
      {
         class3 = Operator.DOUBLE;
      }
      else if (className == Long.class)
      {
         class3 = Operator.LONG;
      }
      else if (className == Integer.class)
      {
         class3 = Operator.LONG;
         arg3 = new Long(((Integer)arg3).longValue());
      }
      else if (className == Short.class)
      {
         class3 = Operator.LONG;
         arg3 = new Long(((Short)arg3).longValue());
      }
      else if (className == Byte.class)
      {
         class3 = Operator.LONG;
         arg3 = new Long(((Byte)arg3).longValue());
      }
      else if (className == Float.class)
      {
         class3 = Operator.DOUBLE;
         arg3 = new Double(((Float)arg3).doubleValue());
      }
      else if (className == Boolean.class)
      {
         class3 = Operator.BOOLEAN;
      }
      else
      {
         throwBadObjectException(className);
      }
   }

   public Object apply() throws Exception
   {
      switch (operation)
      {
         case EQUAL:
            return equal();
         case NOT:
            return not();
         case AND:
            return and();
         case OR:
            return or();
         case GT:
            return gt();
         case GE:
            return ge();
         case LT:
            return lt();
         case LE:
            return le();
         case DIFFERENT:
            return different();
         case ADD:
            return add();
         case SUB:
            return sub();
         case NEG:
            return neg();
         case MUL:
            return mul();
         case DIV:
            return div();
         case BETWEEN:
            return between();
         case NOT_BETWEEN:
            return not_between();
         case LIKE:
            return like(false, false);
         case NOT_LIKE:
            return like(true, false);
         case LIKE_ESCAPE:
            return like(false, true);
         case NOT_LIKE_ESCAPE:
            return like(true, true);
         case IS_NULL:
            return is_null();
         case IS_NOT_NULL:
            return is_not_null();
         case IN:
            return in();
         case NOT_IN:
            return not_in();
      }

      throw new Exception("Unknown operation: " + toString());
   }

   public void throwBadObjectException(final Class class1) throws Exception
   {
      throw new Exception("Bad Object: '" + class1.getName() + "' for operation: " + toString());
   }

   public void throwBadObjectException(final int class1) throws Exception
   {
      throw new Exception("Bad Object: '" + Operator.getClassName(class1) + "' for operation: " + toString());
   }

   public void throwBadObjectException(final int class1, final int class2) throws Exception
   {
      throw new Exception("Bad Object: expected '" + Operator.getClassName(class1) +
                          "' got '" +
                          Operator.getClassName(class2) +
                          "' for operation: " +
                          toString());
   }

   static String getClassName(final int class1)
   {
      String str = "Unknown";
      switch (class1)
      {
         case SIMPLE_STRING:
            str = "SimpleString";
            break;
         case LONG:
            str = "Long";
            break;
         case DOUBLE:
            str = "Double";
            break;
         case BOOLEAN:
            str = "Boolean";
            break;
      }
      return str;
   }

   static String operationString(final int operation)
   {
      String str = "Unknown";
      switch (operation)
      {
         case EQUAL:
            str = "EQUAL";
            break;
         case NOT:
            str = "NOT";
            break;
         case AND:
            str = "AND";
            break;
         case OR:
            str = "OR";
            break;
         case GT:
            str = "GT";
            break;
         case GE:
            str = "GE";
            break;
         case LT:
            str = "LT";
            break;
         case LE:
            str = "LE";
            break;
         case DIFFERENT:
            str = "DIFFERENT";
            break;
         case ADD:
            str = "ADD";
            break;
         case SUB:
            str = "SUB";
            break;
         case NEG:
            str = "NEG";
            break;
         case MUL:
            str = "MUL";
            break;
         case DIV:
            str = "DIV";
            break;
         case BETWEEN:
            str = "BETWEEN";
            break;
         case NOT_BETWEEN:
            str = "NOT_BETWEEN";
            break;
         case LIKE:
            str = "LIKE";
            break;
         case NOT_LIKE:
            str = "NOT_LIKE";
            break;
         case LIKE_ESCAPE:
            str = "LIKE_ESCAPE";
            break;
         case NOT_LIKE_ESCAPE:
            str = "NOT_LIKE_ESCAPE";
            break;
         case IS_NULL:
            str = "IS_NULL";
            break;
         case IS_NOT_NULL:
            str = "IS_NOT_NULL";
            break;
         case IN:
            str = "IN";
            break;
         case NOT_IN:
            str = "NOT_IN";
            break;
      }
      return str;
   }
}
