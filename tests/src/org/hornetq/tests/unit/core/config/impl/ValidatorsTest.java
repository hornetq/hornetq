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

package org.hornetq.tests.unit.core.config.impl;

import static org.hornetq.core.config.impl.Validators.GE_ZERO;
import static org.hornetq.core.config.impl.Validators.GT_ZERO;
import static org.hornetq.core.config.impl.Validators.JOURNAL_TYPE;
import static org.hornetq.core.config.impl.Validators.MINUS_ONE_OR_GE_ZERO;
import static org.hornetq.core.config.impl.Validators.MINUS_ONE_OR_GT_ZERO;
import static org.hornetq.core.config.impl.Validators.NOT_NULL_OR_EMPTY;
import static org.hornetq.core.config.impl.Validators.NO_CHECK;
import static org.hornetq.tests.util.RandomUtil.randomString;
import junit.framework.TestCase;

import org.hornetq.core.config.impl.Validators;
import org.hornetq.core.server.JournalType;

/**
 * A ValidatorsTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class ValidatorsTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   private static void success(Validators.Validator validator, Object value)
   {
      validator.validate(randomString(), value);
   }

   private static void failure(Validators.Validator validator, Object value)
   {
      try
      {
         validator.validate(randomString(), value);
         fail(validator + " must not validate " + value);
      }
      catch (IllegalArgumentException e)
      {

      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGE_ZERO() throws Exception
   {
      failure(GE_ZERO, -1);
      success(GE_ZERO, 0);
      success(GE_ZERO, 0.1);
      success(GE_ZERO, 1);
   }

   public void testGT_ZERO() throws Exception
   {
      failure(GT_ZERO, -1);
      failure(GT_ZERO, 0);
      success(GT_ZERO, 0.1);
      success(GT_ZERO, 1);
   }

   public void testMINUS_ONE_OR_GE_ZERO() throws Exception
   {
      failure(MINUS_ONE_OR_GE_ZERO, -2);
      success(MINUS_ONE_OR_GE_ZERO, -1);
      success(MINUS_ONE_OR_GE_ZERO, 0);
      success(MINUS_ONE_OR_GE_ZERO, 0.1);
      success(MINUS_ONE_OR_GE_ZERO, 1);
   }

   public void testMINUS_ONE_OR_GT_ZERO() throws Exception
   {
      failure(MINUS_ONE_OR_GT_ZERO, -2);
      success(MINUS_ONE_OR_GT_ZERO, -1);
      failure(MINUS_ONE_OR_GT_ZERO, 0);
      success(MINUS_ONE_OR_GT_ZERO, 0.1);
      success(MINUS_ONE_OR_GT_ZERO, 1);
   }

   public void testNO_CHECK() throws Exception
   {
      success(NO_CHECK, -1);
      success(NO_CHECK, null);
      success(NO_CHECK, "");
      success(NO_CHECK, true);
      success(NO_CHECK, false);
   }

   public void testNOT_NULL_OR_EMPTY() throws Exception
   {
      failure(NOT_NULL_OR_EMPTY, null);
      failure(NOT_NULL_OR_EMPTY, "");
      success(NOT_NULL_OR_EMPTY, randomString());
   }
   
   public void testJOURNAL_TYPE() throws Exception
   {
      for(JournalType type: JournalType.values())
      {
         success(JOURNAL_TYPE, type.toString());
      }
      failure(JOURNAL_TYPE, null);
      failure(JOURNAL_TYPE, "");
      failure(JOURNAL_TYPE, randomString());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
