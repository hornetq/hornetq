package org.hornetq.core.registry;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.hornetq.spi.core.naming.BindingRegistry;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class MapBindingRegistry implements BindingRegistry
{
   protected ConcurrentMap<String, Object> registry = new ConcurrentHashMap<String, Object>();

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

   public Object getContext()
   {
      return registry;
   }

   /* (non-Javadoc)
    * @see org.hornetq.spi.core.naming.BindingRegistry#setContext(java.lang.Object)
    */
   public void setContext(Object ctx)
   {
      registry = (ConcurrentMap)ctx;
   }
}
