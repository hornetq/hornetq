package org.hornetq.rest.integration;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.hornetq.jms.client.ConnectionFactoryOptions;
import org.hornetq.rest.MessageServiceManager;
import org.hornetq.utils.ObjectInputStreamWithClassLoader;
import org.jboss.resteasy.spi.Registry;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 */
public class RestMessagingBootstrapListener implements ServletContextListener, ConnectionFactoryOptions
{
   MessageServiceManager manager;
   private String deserializationBlackList;
   private String deserializationWhiteList;

   public void contextInitialized(ServletContextEvent contextEvent)
   {
      ServletContext context = contextEvent.getServletContext();
      Registry registry = (Registry) context.getAttribute(Registry.class.getName());
      if (registry == null)
      {
         throw new RuntimeException("You must install RESTEasy as a Bootstrap Listener and it must be listed before this class");
      }
      String configfile = context.getInitParameter("rest.messaging.config.file");
      deserializationBlackList = context.getInitParameter(ObjectInputStreamWithClassLoader.BLACKLIST_PROPERTY);
      deserializationWhiteList = context.getInitParameter(ObjectInputStreamWithClassLoader.WHITELIST_PROPERTY);
      manager = new MessageServiceManager(this);

      if (configfile != null)
      {
         manager.setConfigResourcePath(configfile);
      }
      try
      {
         manager.start();
         registry.addSingletonResource(manager.getQueueManager().getDestination());
         registry.addSingletonResource(manager.getTopicManager().getDestination());
      }
      catch (Exception e)
      {
         throw new RuntimeException(e);
      }
   }

   public void contextDestroyed(ServletContextEvent servletContextEvent)
   {
      if (manager != null)
      {
         manager.stop();
      }
   }

   @Override
   public String getDeserializationBlackList()
   {
      return deserializationBlackList;
   }

   @Override
   public void setDeserializationBlackList(String blackList)
   {
      deserializationBlackList = blackList;
   }

   @Override
   public String getDeserializationWhiteList()
   {
      return deserializationWhiteList;
   }

   @Override
   public void setDeserializationWhiteList(String whiteList)
   {
      deserializationWhiteList = whiteList;
   }
}