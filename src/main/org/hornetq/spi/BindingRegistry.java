package org.hornetq.spi;

/**
 * Abstract interface for a registry to store endpoints like connection factories into.
 *
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public interface BindingRegistry
{
   Object lookup(String name);
   boolean bind(String name, Object obj);
   void unbind(String name);
   void close();
}
