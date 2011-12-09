package org.hornetq.rest.integration;

import org.hornetq.spi.core.naming.BindingRegistry;

import javax.servlet.ServletContext;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class ServletContextBindingRegistry implements BindingRegistry
{
   private ServletContext servletContext;

   public ServletContextBindingRegistry(ServletContext servletContext)
   {
      this.servletContext = servletContext;
   }

   public Object lookup(String name)
   {
      return servletContext.getAttribute(name);
   }

   public boolean bind(String name, Object obj)
   {
      servletContext.setAttribute(name, obj);
      return true;
   }

   public void unbind(String name)
   {
      servletContext.removeAttribute(name);
   }

   public void close()
   {
   }

   public Object getContext()
   {
      return servletContext;
   }

   public void setContext(Object o)
   {
      servletContext = (ServletContext)o;
   }
}
