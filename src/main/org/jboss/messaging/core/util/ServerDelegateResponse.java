/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.util;

import org.jgroups.Address;

import java.io.Serializable;

/**
 * A carrier for a response coming from a single sub-server object. Instances of this class will
 * be sent over the network.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ServerDelegateResponse implements Serializable
{
   // Attributes ----------------------------------------------------

   protected Serializable subServerID;
   protected Object result;

   // Constructors --------------------------------------------------

   public ServerDelegateResponse(Serializable subServerID, Object result)
   {
      this.subServerID = subServerID;
      this.result = result;
   }

   // Public --------------------------------------------------------

   public Serializable getSubServerID()
   {
      return subServerID;
   }

   /**
    * Return the result as it was returned by the remote sub-server (it can be null), or a
    * Throwable, if the remote invocation generated an exception.
    *
    * @return - the result, null or a Throwable.
    */
   public Object getInvocationResult()
   {
      return result;
   }
}
