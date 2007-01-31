/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core.plugin.postoffice.cluster;

import org.jboss.messaging.core.plugin.postoffice.cluster.jchannelfactory.JChannelFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.jchannelfactory.MultiplexerJChannelFactory;
import org.jboss.logging.Logger;
import org.jboss.test.messaging.tools.jboss.ServiceDeploymentDescriptor;
import org.jboss.test.messaging.tools.jboss.MBeanConfigurationElement;
import org.jboss.jms.util.XMLUtil;
import org.jgroups.JChannel;
import org.w3c.dom.Element;

import javax.management.ObjectName;
import javax.management.MBeanServer;
import java.net.URL;

/**
 * A JChannelFactory that reads the configuration of its synchronous/asynchronous JChannels from a
 * Messaging-style clustered persistence service configuration file (usually shipped with a
 * Messaging installation). The idea is to test with priority whatever we ship.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClusteredPersistenceServiceConfigFileJChannelFactory implements JChannelFactory
{
   // Constants ------------------------------------------------------------------------------------

   public static final Logger log =
      Logger.getLogger(ClusteredPersistenceServiceConfigFileJChannelFactory.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private String configFilePath;

   // we're either using a delegate MultiplexerJChannelFactory, if we find one configured in the
   // file ...
   private JChannelFactory multiplexorDelegate;

   // ... or just plain XML configuration.
   private Element syncConfig;
   private Element asyncConfig;

   // Constructors ---------------------------------------------------------------------------------

   /**
    * @param configFilePath - the configuration file to read JGroups stack configurations from. Must
    *        be relative to classpath components in order to be found.
    * @param skipMultiplex - if true, ignore multiplex option, even if a channel factory name is
    *        found in the configuration file. Otherwise, the channel factory will take priority
    *        if found.
    * @param mbeanServer - the MBeanServer instance, needed in case a channel factory name is found
    *        in the configuration file. In this situation, the channel factory is preferred.
    *        Irrelevant if skipMultiplex is true.
    */
   public ClusteredPersistenceServiceConfigFileJChannelFactory(String configFilePath,
                                                               boolean skipMultiplex,
                                                               MBeanServer mbeanServer)
      throws Exception
   {
      this.configFilePath = configFilePath;
      init(configFilePath, skipMultiplex, mbeanServer);
   }

   // JChannelFactory ------------------------------------------------------------------------------

   public JChannel createSyncChannel() throws Exception
   {
      if (multiplexorDelegate != null)
      {
         return multiplexorDelegate.createSyncChannel();
      }
      else
      {
         return new JChannel(syncConfig);
      }
   }

   public JChannel createASyncChannel() throws Exception
   {
      if (multiplexorDelegate != null)
      {
         return multiplexorDelegate.createASyncChannel();
      }
      else
      {
         return new JChannel(asyncConfig);
      }
   }

   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "ClusteredPersistenceServiceConfigFileJChannelFactory[" + configFilePath + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private void init(String configFilePath, boolean skipMultiplex, MBeanServer mbeanServer)
      throws Exception
   {
      log.debug("using configuration file " + configFilePath);

      URL configFileURL = getClass().getClassLoader().getResource(configFilePath);

      if (configFileURL == null)
      {
         throw new Exception("Cannot find " + configFilePath + " in the classpath");
      }

      ServiceDeploymentDescriptor pdd = new ServiceDeploymentDescriptor(configFileURL);

      MBeanConfigurationElement postOfficeConfig =
         (MBeanConfigurationElement)pdd.query("service", "PostOffice").iterator().next();

      // first, we try to use a channel factory service, if we find one configured
      String s = (String)postOfficeConfig.getAttributeValue("ChannelFactoryName");

      if (s != null && !skipMultiplex)
      {
         // there's a chance we can use a multiplexer service
         ObjectName channelFactoryName = new ObjectName(s);

         String channelPartitionName =
            (String)postOfficeConfig.getAttributeValue("ChannelPartitionName");

         if (channelPartitionName == null)
         {
            throw new IllegalStateException("Cannot find ChannelPartitionName");
         }

         String syncChannelName = (String)postOfficeConfig.getAttributeValue("SyncChannelName");

         if (syncChannelName == null)
         {
            throw new IllegalStateException("Cannot find SyncChannelName");
         }

         String asyncChannelName = (String)postOfficeConfig.getAttributeValue("AsyncChannelName");

         if (asyncChannelName == null)
         {
            throw new IllegalStateException("Cannot find AsyncChannelName");
         }

         try
         {
            if(mbeanServer.getMBeanInfo(channelFactoryName) != null)
            {
               multiplexorDelegate =
                  new MultiplexerJChannelFactory(mbeanServer, channelFactoryName,
                                                 channelPartitionName, syncChannelName,
                                                 asyncChannelName);

               // initialization ends here, we've found what we were looking for
               return;
            }
         }
         catch (Exception e)
         {
            // that's alright, no multiplexer there, use the regular XML configuration
            log.debug("Wasn't able to find " + s);
         }
      }

      // the only chance now is to use the XML configurations

      s = (String)postOfficeConfig.getAttributeValue("SyncChannelConfig");

      if (s == null)
      {
         throw new IllegalStateException("Cannot find SyncChannelConfig");
      }

      syncConfig = XMLUtil.stringToElement(s);

      s = (String)postOfficeConfig.getAttributeValue("AsyncChannelConfig");

      if (s == null)
      {
         throw new IllegalStateException("Cannot find AsyncChannelConfig");
      }

      asyncConfig = XMLUtil.stringToElement(s);
   }

   // Inner classes --------------------------------------------------------------------------------

}
