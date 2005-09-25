/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.local;

import org.jboss.messaging.core.Distributor;
import org.jboss.messaging.core.Receiver;

public abstract class Destination implements Distributor, Receiver
{
   protected String name;
   
   public String getName()
   {
      return name;
   }
     
}
