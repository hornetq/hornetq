/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core.postoffice;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.ChannelFactory;
import org.jboss.messaging.util.XMLUtil;
import org.jboss.test.messaging.tools.container.ServiceConfigHelper;
import org.jboss.test.messaging.tools.jboss.MBeanConfigurationElement;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.w3c.dom.Element;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * A JChannelFactory that reads the configuration of its synchronous/asynchronous JChannels from a
 * Messaging-style clustered persistence service configuration file (usually shipped with a
 * Messaging installation). The idea is to test with priority whatever we ship.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClusteredPersistenceServiceConfigFileJChannelFactory implements ChannelFactory
{
   // Constants ------------------------------------------------------------------------------------

   public static final Logger log =
      Logger.getLogger(ClusteredPersistenceServiceConfigFileJChannelFactory.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private String configFilePath;

   // we're either using a delegate MultiplexerJChannelFactory, if we find one configured in the
   // file ...
   private ChannelFactory multiplexorDelegate;

   // ... or just plain XML configuration.
   private Element controlConfig;
   private Element dataConfig;

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

   // ChannelFactory ------------------------------------------------------------------------------

   public Channel createControlChannel() throws Exception
   {
      if (multiplexorDelegate != null)
      {
         return multiplexorDelegate.createControlChannel();
      }
      else
      {
         return new JChannel(controlConfig);
      }
   }

   public Channel createDataChannel() throws Exception
   {
      if (multiplexorDelegate != null)
      {
         return multiplexorDelegate.createDataChannel();
      }
      else
      {
         return new JChannel(dataConfig);
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

      MBeanConfigurationElement postOfficeConfig =
         ServiceConfigHelper.loadServiceConfiguration(configFilePath, "PostOffice");

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

         String controlChannelName = (String)postOfficeConfig.getAttributeValue("ControlChannelName");

         if (controlChannelName == null)
         {
            throw new IllegalStateException("Cannot find ControlChannelName");
         }

         String dataChannelName = (String)postOfficeConfig.getAttributeValue("DataChannelName");

         if (dataChannelName == null)
         {
            throw new IllegalStateException("Cannot find DataChannelName");
         }

         try
         {
            if(mbeanServer.getMBeanInfo(channelFactoryName) != null)
            {
               /*multiplexorDelegate =
                  new MultiplexerChannelFactory(mbeanServer, channelFactoryName,
                                                 channelPartitionName, controlChannelName,
                                                 dataChannelName);*/

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

      s = (String)postOfficeConfig.getAttributeValue("ControlChannelConfig");

      if (s == null)
      {
         throw new IllegalStateException("Cannot find ControlChannelConfig");
      }

      controlConfig = XMLUtil.stringToElement(s);

      s = (String)postOfficeConfig.getAttributeValue("DataChannelConfig");

      if (s == null)
      {
         throw new IllegalStateException("Cannot find DataChannelConfig");
      }

      dataConfig = XMLUtil.stringToElement(s);
   }

   // Inner classes --------------------------------------------------------------------------------

}
