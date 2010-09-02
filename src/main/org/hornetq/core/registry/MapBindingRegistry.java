package org.hornetq.core.registry;

import org.hornetq.spi.BindingRegistry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class MapBindingRegistry implements BindingRegistry
{
   protected ConcurrentHashMap<String, Object> registry = new ConcurrentHashMap<String, Object>();

   public Object lookup(String name)
   {
      return registry.get(name);
   }

   public boolean bind(String name, Object obj)
   {
      return registry.putIfAbsent(name, obj) == null;
   }

   public void unbind(String name)
   {
      registry.remove(name);
   }

   public void close()
   {
   }
}
