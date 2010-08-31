package org.hornetq.integration.spring;

import org.hornetq.spi.BindingRegistry;
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
      return factory.getBean(name);
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
}
