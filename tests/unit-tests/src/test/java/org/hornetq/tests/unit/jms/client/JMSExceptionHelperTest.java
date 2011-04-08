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

package org.hornetq.tests.unit.jms.client;

import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.jms.client.JMSExceptionHelper;
import org.hornetq.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JMSExceptionHelperTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCONNECTION_TIMEDOUT() throws Exception
   {
      doConvertException(HornetQException.CONNECTION_TIMEDOUT, JMSException.class);
   }

   public void testILLEGAL_STATE() throws Exception
   {
      doConvertException(HornetQException.ILLEGAL_STATE, IllegalStateException.class);
   }

   public void testINTERNAL_ERROR() throws Exception
   {
      doConvertException(HornetQException.INTERNAL_ERROR, JMSException.class);
   }

   public void testINVALID_FILTER_EXPRESSION() throws Exception
   {
      doConvertException(HornetQException.INVALID_FILTER_EXPRESSION, InvalidSelectorException.class);
   }

   public void testNOT_CONNECTED() throws Exception
   {
      doConvertException(HornetQException.NOT_CONNECTED, JMSException.class);
   }

   public void testOBJECT_CLOSED() throws Exception
   {
      doConvertException(HornetQException.OBJECT_CLOSED, IllegalStateException.class);
   }

   public void testQUEUE_DOES_NOT_EXIST() throws Exception
   {
      doConvertException(HornetQException.QUEUE_DOES_NOT_EXIST, InvalidDestinationException.class);
   }

   public void testQUEUE_EXISTS() throws Exception
   {
      doConvertException(HornetQException.QUEUE_EXISTS, InvalidDestinationException.class);
   }

   public void testSECURITY_EXCEPTION() throws Exception
   {
      doConvertException(HornetQException.SECURITY_EXCEPTION, JMSSecurityException.class);
   }

   public void testUNSUPPORTED_PACKET() throws Exception
   {
      doConvertException(HornetQException.UNSUPPORTED_PACKET, IllegalStateException.class);
   }

   public void testDefault() throws Exception
   {
      int invalidErrorCode = 2000;
      doConvertException(invalidErrorCode, JMSException.class);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void doConvertException(final int errorCode, final Class expectedException)
   {
      HornetQException me = new HornetQException(errorCode);
      Exception e = JMSExceptionHelper.convertFromHornetQException(me);
      Assert.assertNotNull(e);
      Assert.assertTrue(e.getClass().isAssignableFrom(expectedException));
   }

   // Inner classes -------------------------------------------------
}
