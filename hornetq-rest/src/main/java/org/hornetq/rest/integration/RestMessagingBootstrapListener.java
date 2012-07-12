package org.hornetq.rest.integration;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.hornetq.rest.MessageServiceManager;
import org.jboss.resteasy.spi.Registry;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 */
public class RestMessagingBootstrapListener implements ServletContextListener
{
   MessageServiceManager manager;

   public void contextInitialized(ServletContextEvent contextEvent)
   {
      ServletContext context = contextEvent.getServletContext();
      String configfile = context.getInitParameter("rest.messaging.config.file");
      Registry registry = (Registry) context.getAttribute(Registry.class.getName());
      if (registry == null)
      {
         throw new RuntimeException("You must install RESTEasy as a Bootstrap Listener and it must be listed before this class");
      }
      manager = new MessageServiceManager();

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
}