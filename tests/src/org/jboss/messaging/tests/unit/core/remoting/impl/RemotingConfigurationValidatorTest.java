/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.core.remoting.impl;

import junit.framework.TestCase;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import static org.jboss.messaging.core.remoting.TransportType.INVM;
import static org.jboss.messaging.core.remoting.impl.RemotingConfigurationValidator.validate;

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

   public void testINVMConfiguration()
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(INVM);

      validate(config);
   }

   public void testNegativePort()
   {
      Configuration config = ConfigurationHelper.newTCPConfiguration("localhost", -1);

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
      ConfigurationImpl config = ConfigurationHelper.newTCPConfiguration("localhost", 9000);
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
      ConfigurationImpl config = ConfigurationHelper.newTCPConfiguration("localhost", 9000);
      config.getConnectionParams().setTcpReceiveBufferSize(-1);
      validate(config);
   }

   public void test_TcpReceiveBufferSize_to_NegativeNumber()
   {
      ConfigurationImpl config = ConfigurationHelper.newTCPConfiguration("localhost", 9000);
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
      ConfigurationImpl config = ConfigurationHelper.newTCPConfiguration("localhost", 9000);
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
      ConfigurationImpl config = ConfigurationHelper.newTCPConfiguration("localhost", 9000);
      config.getConnectionParams().setTcpSendBufferSize(-1);
      validate(config);
   }

   public void test_TcpSendBufferSize_to_NegativeNumber()
   {
      ConfigurationImpl config = ConfigurationHelper.newTCPConfiguration("localhost", 9000);
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
      config.getConnectionParams().setInVMDisabled(true);

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
