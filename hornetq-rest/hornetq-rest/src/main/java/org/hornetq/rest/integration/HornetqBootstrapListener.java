package org.hornetq.rest.integration;

import org.hornetq.jms.server.embedded.EmbeddedJMS;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class HornetqBootstrapListener implements ServletContextListener
{
   private EmbeddedJMS jms;

   public void contextInitialized(ServletContextEvent contextEvent)
   {
      ServletContext context = contextEvent.getServletContext();
      jms = new EmbeddedJMS();
      jms.setRegistry(new ServletContextBindingRegistry(context));
      try
      {
         jms.start();
      }
      catch (Exception e)
      {
         throw new RuntimeException(e);
      }
   }

   public void contextDestroyed(ServletContextEvent servletContextEvent)
   {
      try
      {
         if (jms != null) jms.stop();
      }
      catch (Exception e)
      {
         throw new RuntimeException(e);
      }
   }
}
