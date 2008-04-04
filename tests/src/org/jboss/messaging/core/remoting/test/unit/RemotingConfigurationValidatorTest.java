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

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.remoting.impl.ConfigurationHelper;

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
      Configuration config = ConfigurationHelper.newConfiguration(INVM, "localhost", -1);
      
      validate(config);
   }

   public void testNegativePort()
   {
      Configuration config =  ConfigurationHelper.newConfiguration(TCP, "localhost", -1);

      try 
      {
         validate(config);
         fail("can not set a negative port");
      } catch (Exception e)
      {
         
      }
   }
   
   public void test_DisableINVM_With_INVMTransport()
   {
      Configuration config = ConfigurationHelper.newConfiguration(INVM, "localhost", 9000);
      config.setInvmDisabled(true);
      
      try 
      {
         validate(config);
         fail("can not disable INVM when INVM transport is set");
      } catch (Exception e)
      {
         
      }
   }
   
   public void test_EnableSSL_With_INVMTransport()
   {
      Configuration config = ConfigurationHelper.newConfiguration(INVM, "localhost", 9000);
      config.setSSLEnabled(true);
      
      try 
      {
         validate(config);
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
