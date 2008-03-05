/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.test.unit;

import static org.jboss.messaging.core.remoting.TransportType.INVM;
import static org.jboss.messaging.core.remoting.TransportType.TCP;
import static org.jboss.messaging.core.remoting.impl.RemotingConfigurationValidator.validate;
import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.RemotingConfiguration;
import org.jboss.messaging.core.remoting.impl.RemotingConfigurationImpl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
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
      RemotingConfiguration conf = RemotingConfigurationImpl.newINVMConfiguration();

      validate(conf);
   }

   public void testNegativePort()
   {
      RemotingConfiguration conf = new RemotingConfigurationImpl(TCP, "localhost", -1);

      try 
      {
         validate(conf);
         fail("can not set a negative port");
      } catch (Exception e)
      {
         
      }
   }
   
   public void test_DisableINVM_With_INVMTransport()
   {
      RemotingConfigurationImpl conf = new RemotingConfigurationImpl(INVM, "localhost", 9000);
      conf.setInvmDisabled(true);
      
      try 
      {
         validate(conf);
         fail("can not disable INVM when INVM transport is set");
      } catch (Exception e)
      {
         
      }
   }
   
   public void test_EnableSSL_With_INVMTransport()
   {
      RemotingConfigurationImpl conf = new RemotingConfigurationImpl(INVM, "localhost", 9000);
      conf.setSSLEnabled(true);
      
      try 
      {
         validate(conf);
         fail("can not enable SSL when INVM transport is set");
      } catch (Exception e)
      {
         
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
