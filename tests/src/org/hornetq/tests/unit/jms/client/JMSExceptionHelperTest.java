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

import org.hornetq.core.exception.MessagingException;
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
      doConvertException(MessagingException.CONNECTION_TIMEDOUT,
            JMSException.class);
   }

   public void testILLEGAL_STATE() throws Exception
   {
      doConvertException(MessagingException.ILLEGAL_STATE,
            IllegalStateException.class);
   }

   public void testINTERNAL_ERROR() throws Exception
   {
      doConvertException(MessagingException.INTERNAL_ERROR,
            JMSException.class);
   }

   public void testINVALID_FILTER_EXPRESSION() throws Exception
   {
      doConvertException(MessagingException.INVALID_FILTER_EXPRESSION,
            InvalidSelectorException.class);
   }

   public void testNOT_CONNECTED() throws Exception
   {
      doConvertException(MessagingException.NOT_CONNECTED,
            JMSException.class);
   }

   public void testOBJECT_CLOSED() throws Exception
   {
      doConvertException(MessagingException.OBJECT_CLOSED,
            IllegalStateException.class);
   }

   public void testQUEUE_DOES_NOT_EXIST() throws Exception
   {
      doConvertException(MessagingException.QUEUE_DOES_NOT_EXIST,
            InvalidDestinationException.class);
   }

   public void testQUEUE_EXISTS() throws Exception
   {
      doConvertException(MessagingException.QUEUE_EXISTS,
            InvalidDestinationException.class);
   }

   public void testSECURITY_EXCEPTION() throws Exception
   {
      doConvertException(MessagingException.SECURITY_EXCEPTION,
            JMSSecurityException.class);
   }

   public void testUNSUPPORTED_PACKET() throws Exception
   {
      doConvertException(MessagingException.UNSUPPORTED_PACKET,
            IllegalStateException.class);
   }

   public void testDefault() throws Exception
   {
      int invalidErrorCode = 2000;
      doConvertException(invalidErrorCode, JMSException.class);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void doConvertException(int errorCode, Class expectedException)
   {
      MessagingException me = new MessagingException(errorCode);
      Exception e = JMSExceptionHelper.convertFromMessagingException(me);
      assertNotNull(e);
      assertTrue(e.getClass().isAssignableFrom(expectedException));
   }

   // Inner classes -------------------------------------------------
}
