/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.config.impl;

import static java.lang.String.format;

import org.jboss.messaging.core.server.JournalType;

/**
 * A Validators
 *
 * @author jmesnil
 *
 *
 */
public class Validators
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------
   
   public static interface Validator
   {
      void validate(String name, Object value);
   }
   
   public static Validator NO_CHECK = new Validator()
   {
      public void validate(String name, Object value)
      {
         return;
      }
   };
   
   public static Validator NOT_NULL_OR_EMPTY = new Validator()
   {
      public void validate(String name, Object value)
      {
         String str = (String)value;
         if (str == null || str.length() == 0)
         {
            throw new IllegalArgumentException(format("%s must neither be null nor empty", name));
         }
      }
   };

   public static Validator GT_ZERO = new Validator()
   {
      public void validate(String name, Object value)
      {
         Number val = (Number)value;
         if (val.doubleValue() > 0)
         {
            // OK
         }
         else
         {
            throw new IllegalArgumentException(format("%s  must be greater than 0 (actual value: %s", name, val));
         }
      }
   };

   public static Validator GE_ZERO = new Validator()
   {
      public void validate(String name, Object value)
      {
         Number val = (Number)value;
         if (val.doubleValue() >= 0)
         {
            // OK
         }
         else
         {
            throw new IllegalArgumentException(format("%s  must be greater or equals than 0 (actual value: %s",
                                                      name,
                                                      val));
         }
      }
   };

   public static Validator MINUS_ONE_OR_GT_ZERO = new Validator()
   {
      public void validate(String name, Object value)
      {
         Number val = (Number)value;
         if (val.doubleValue() == -1 || val.doubleValue() > 0)
         {
            // OK
         }
         else
         {
            throw new IllegalArgumentException(format("%s  must be equals to -1 or greater than 0 (actual value: %s)",
                                                      name,
                                                      val));
         }
      }
   };

   public static Validator MINUS_ONE_OR_GE_ZERO = new Validator()
   {
      public void validate(String name, Object value)
      {
         Number val = (Number)value;
         if (val.doubleValue() == -1 || val.doubleValue() >= 0)
         {
            // OK
         }
         else
         {
            throw new IllegalArgumentException(format("%s  must be equals to -1 or greater or equals to 0 (actual value: %s)",
                                                      name,
                                                      val));
         }
      }
   };

   public static final Validator THREAD_PRIORITY_RANGE = new Validator()
   {
      public void validate(String name, Object value)
      {
         Number val = (Number)value;
         if (val.intValue() >= Thread.MIN_PRIORITY && val.intValue() <= Thread.MAX_PRIORITY)
         {
            // OK
         }
         else
         {
            throw new IllegalArgumentException(format("%s must be betwen %s and %s inclusive (actual value: %s)",
                                                      name,
                                                      Thread.MIN_PRIORITY,
                                                      Thread.MAX_PRIORITY,
                                                      value));
         }
      }
   };

   public static final Validator JOURNAL_TYPE = new Validator()
   {
      public void validate(String name, Object value)
      {
         String val = (String)value;
         if (val == null || !val.equals(JournalType.NIO.toString()) && !val.equals(JournalType.ASYNCIO.toString()))
         {
            throw new IllegalArgumentException("Invalid journal type " + val);
         }
      }
   };

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
