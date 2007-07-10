/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.plugin;

import org.jboss.jms.delegate.ConnectionFactoryDelegate;

import java.io.Serializable;

/**
 * The interface that must be implemented by any load balancing policy plugin.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public interface LoadBalancingPolicy extends Serializable
{
   static final long serialVersionUID = 328573973957394573L;

   ConnectionFactoryDelegate getNext();
   
   /**
    * This method should be called when updating the LoadBalancingFactory
    * @param delegates - a List<ConnectionFactoryDelegate> representing the lastest cluster view
    *        to chose delegates from
    */
   void updateView(ConnectionFactoryDelegate[] delegates);


}
