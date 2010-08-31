package org.hornetq.core.server.embedded;

import org.hornetq.core.config.impl.FileConfiguration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.spi.core.security.HornetQSecurityManager;
import org.hornetq.spi.core.security.HornetQSecurityManagerImpl;

import javax.management.MBeanServer;

/**
 * Helper class to simplify bootstrap of HornetQ server.  Bootstraps from classpath-based config files.
 *
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class EmbeddedHornetQ
{
   protected HornetQSecurityManager securityManager;
   protected FileConfiguration configuration = new FileConfiguration();
   protected HornetQServer hornetQServer;
   protected MBeanServer mbeanServer;

   /**
    * Classpath resource for hornetq server config.  Defaults to 'hornetq-configuration.xml'.
    *
    * @param filename
    */
   public void setConfigResourcePath(String filename)
   {
      configuration.setConfigurationUrl(filename);
   }

   /**
    * Set the hornetq security manager.  This defaults to org.hornetq.spi.core.security.HornetQSecurityManagerImpl
    *
    * @param securityManager
    */
   public void setSecurityManager(HornetQSecurityManager securityManager)
   {
      this.securityManager = securityManager;
   }

   /**
    * Use this mbean server to register management beans.  If not set, no mbeans will be registered.
    *
    * @param mbeanServer
    */
   public void setMbeanServer(MBeanServer mbeanServer)
   {
      this.mbeanServer = mbeanServer;
   }

   public HornetQServer getHornetQServer()
   {
      return hornetQServer;
   }

   public void start() throws Exception
   {
      initStart();
      hornetQServer.start();

   }

   protected void initStart() throws Exception
   {
      configuration.start();
      if (securityManager == null)
      {
         securityManager = new HornetQSecurityManagerImpl();
      }
      if (mbeanServer == null)
      {
         hornetQServer = new HornetQServerImpl(configuration, securityManager);
      }
      else
      {
         hornetQServer = new HornetQServerImpl(configuration, mbeanServer, securityManager);
      }
   }

   public void stop() throws Exception
   {
      hornetQServer.stop();
   }
}
