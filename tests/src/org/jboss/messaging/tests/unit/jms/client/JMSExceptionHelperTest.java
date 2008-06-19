/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.unit.jms.client;

import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;

import junit.framework.TestCase;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.jms.client.JMSExceptionHelper;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JMSExceptionHelperTest extends TestCase
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
