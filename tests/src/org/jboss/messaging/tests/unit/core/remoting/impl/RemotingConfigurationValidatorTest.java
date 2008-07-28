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

package org.jboss.messaging.tests.unit.core.remoting.impl;

import static org.jboss.messaging.core.remoting.TransportType.INVM;
import static org.jboss.messaging.core.remoting.impl.RemotingConfigurationValidator.validate;
import junit.framework.TestCase;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt>
 */
public class RemotingConfigurationValidatorTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   public void testNullonfigurationThrowsException()
   {
      try
      {
         validate(null);
         fail("should throw exception");
      }
      catch (IllegalStateException e)
      {
         //pass
      }
   }

   public void testINVMConfiguration()
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(INVM);

      validate(config);
   }

   public void testNegativePort()
   {
      Configuration config = new ConfigurationImpl();
      config.setPort(-1);

      try
      {
         validate(config);
         fail("can not set a negative port");
      }
      catch (Exception e)
      {
      }
   }

   public void test_TcpReceiveBufferSize_to_0()
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.getConnectionParams().setTcpReceiveBufferSize(0);
      try
      {
         validate(config);
         fail("can not set tcp receive buffer size to 0");
      }
      catch (Exception e)
      {
      }
   }

   public void test_TcpReceiveBufferSize_to_minusOne()
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.getConnectionParams().setTcpReceiveBufferSize(-1);
      validate(config);
   }

   public void test_TcpReceiveBufferSize_to_NegativeNumber()
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.getConnectionParams().setTcpReceiveBufferSize(-2);
      try
      {
         validate(config);
         fail("can not set tcp receive buffer size to a negative number other than -1");
      }
      catch (Exception e)
      {
      }
   }

   public void test_TcpSendBufferSize_to_0()
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.getConnectionParams().setTcpSendBufferSize(0);
      try
      {
         validate(config);
         fail("can not set tcp send buffer size to 0");
      }
      catch (Exception e)
      {
      }
   }

   public void test_TcpSendBufferSize_to_minusOne()
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.getConnectionParams().setTcpSendBufferSize(-1);
      validate(config);
   }

   public void test_TcpSendBufferSize_to_NegativeNumber()
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.getConnectionParams().setTcpSendBufferSize(-2);
      try
      {
         validate(config);
         fail("can not set tcp send buffer size to a negative number other than -1");
      }
      catch (Exception e)
      {
      }
   }

   public void test_DisableINVM_With_INVMTransport()
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(INVM);
      config.getConnectionParams().setInVMOptimisationEnabled(false);

      try
      {
         validate(config);
         fail("can not disable INVM when INVM transport is set");
      }
      catch (Exception e)
      {

      }
   }

   public void test_EnableSSL_With_INVMTransport()
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(INVM);
      config.setSSLEnabled(true);

      try
      {
         validate(config);
         fail("can not enable SSL when INVM transport is set");
      }
      catch (Exception e)
      {

      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
