package org.hornetq.integration.spring;

import org.hornetq.spi.core.naming.BindingRegistry;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class SpringBindingRegistry implements BindingRegistry
{
   private ConfigurableBeanFactory factory;

   public SpringBindingRegistry(ConfigurableBeanFactory factory)
   {
      this.factory = factory;
   }

   public Object lookup(String name)
   {
      Object obj = null;
      try
      {
         obj = factory.getBean(name);
      }
      catch (NoSuchBeanDefinitionException e)
      {
         //ignore
      }
      return obj;
   }

   public boolean bind(String name, Object obj)
   {
      factory.registerSingleton(name, obj);
      return true;
   }

   public void unbind(String name)
   {
   }

   public void close()
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.spi.core.naming.BindingRegistry#getContext()
    */
   public Object getContext()
   {
      return this.factory;
   }

   /* (non-Javadoc)
    * @see org.hornetq.spi.core.naming.BindingRegistry#setContext(java.lang.Object)
    */
   public void setContext(Object ctx)
   {
      this.factory = (ConfigurableBeanFactory) ctx;
   }
}
