package org.hornetq.spi.core.naming;

/**
 * Abstract interface for a registry to store endpoints like connection factories into.
 *
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public interface BindingRegistry
{
   /** The context used by the registry.
    *   This may be used to setup the JNDI Context on the JNDI Registry.
    *   We keep it as an object here as the interface needs to be generic
    *   as this could be reused by others Registries (e.g set/get the Map on MapRegistry)
    * @return
    */
   Object getContext();
   
   void setContext(Object ctx);
   
   Object lookup(String name);

   boolean bind(String name, Object obj);

   void unbind(String name);

   void close();
}
