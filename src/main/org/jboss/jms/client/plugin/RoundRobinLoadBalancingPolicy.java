/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.plugin;

import org.jboss.jms.delegate.ConnectionFactoryDelegate;

import java.util.List;
import java.util.ArrayList;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RoundRobinLoadBalancingPolicy implements LoadBalancingPolicy
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // The index of the next delegate to be used
   private int next;

   // List<ConnectionFactoryDelegate>
   private List view;

   // Constructors ---------------------------------------------------------------------------------

   // LoadBalancingPolicy implementation -----------------------------------------------------------

   public synchronized ConnectionFactoryDelegate getNext()
   {
      if (next >= view.size())
      {
         next = 0;
      }

      return (ConnectionFactoryDelegate)view.get(next++);
   }

   public synchronized void updateView(List delegates)
   {
      view = new ArrayList(delegates);
      next = 0;
   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
