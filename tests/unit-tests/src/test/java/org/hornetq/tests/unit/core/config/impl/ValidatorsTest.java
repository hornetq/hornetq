/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.unit.core.config.impl;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.hornetq.core.config.impl.Validators;
import org.hornetq.core.server.JournalType;
import org.hornetq.tests.util.RandomUtil;

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

   private static void success(final Validators.Validator validator, final Object value)
   {
      validator.validate(RandomUtil.randomString(), value);
   }

   private static void failure(final Validators.Validator validator, final Object value)
   {
      try
      {
         validator.validate(RandomUtil.randomString(), value);
         Assert.fail(validator + " must not validate " + value);
      }
      catch (IllegalArgumentException e)
      {

      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGE_ZERO() throws Exception
   {
      ValidatorsTest.failure(Validators.GE_ZERO, -1);
      ValidatorsTest.success(Validators.GE_ZERO, 0);
      ValidatorsTest.success(Validators.GE_ZERO, 0.1);
      ValidatorsTest.success(Validators.GE_ZERO, 1);
   }

   public void testGT_ZERO() throws Exception
   {
      ValidatorsTest.failure(Validators.GT_ZERO, -1);
      ValidatorsTest.failure(Validators.GT_ZERO, 0);
      ValidatorsTest.success(Validators.GT_ZERO, 0.1);
      ValidatorsTest.success(Validators.GT_ZERO, 1);
   }

   public void testMINUS_ONE_OR_GE_ZERO() throws Exception
   {
      ValidatorsTest.failure(Validators.MINUS_ONE_OR_GE_ZERO, -2);
      ValidatorsTest.success(Validators.MINUS_ONE_OR_GE_ZERO, -1);
      ValidatorsTest.success(Validators.MINUS_ONE_OR_GE_ZERO, 0);
      ValidatorsTest.success(Validators.MINUS_ONE_OR_GE_ZERO, 0.1);
      ValidatorsTest.success(Validators.MINUS_ONE_OR_GE_ZERO, 1);
   }

   public void testMINUS_ONE_OR_GT_ZERO() throws Exception
   {
      ValidatorsTest.failure(Validators.MINUS_ONE_OR_GT_ZERO, -2);
      ValidatorsTest.success(Validators.MINUS_ONE_OR_GT_ZERO, -1);
      ValidatorsTest.failure(Validators.MINUS_ONE_OR_GT_ZERO, 0);
      ValidatorsTest.success(Validators.MINUS_ONE_OR_GT_ZERO, 0.1);
      ValidatorsTest.success(Validators.MINUS_ONE_OR_GT_ZERO, 1);
   }

   public void testNO_CHECK() throws Exception
   {
      ValidatorsTest.success(Validators.NO_CHECK, -1);
      ValidatorsTest.success(Validators.NO_CHECK, null);
      ValidatorsTest.success(Validators.NO_CHECK, "");
      ValidatorsTest.success(Validators.NO_CHECK, true);
      ValidatorsTest.success(Validators.NO_CHECK, false);
   }

   public void testNOT_NULL_OR_EMPTY() throws Exception
   {
      ValidatorsTest.failure(Validators.NOT_NULL_OR_EMPTY, null);
      ValidatorsTest.failure(Validators.NOT_NULL_OR_EMPTY, "");
      ValidatorsTest.success(Validators.NOT_NULL_OR_EMPTY, RandomUtil.randomString());
   }

   public void testJOURNAL_TYPE() throws Exception
   {
      for (JournalType type : JournalType.values())
      {
         ValidatorsTest.success(Validators.JOURNAL_TYPE, type.toString());
      }
      ValidatorsTest.failure(Validators.JOURNAL_TYPE, null);
      ValidatorsTest.failure(Validators.JOURNAL_TYPE, "");
      ValidatorsTest.failure(Validators.JOURNAL_TYPE, RandomUtil.randomString());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
