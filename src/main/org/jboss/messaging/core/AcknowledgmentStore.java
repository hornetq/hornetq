/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import java.io.Serializable;

/**
 * An AcknowledgmentStore is a reliable repository for negative acknowledgments.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface AcknowledgmentStore
{
   public Serializable getStoreID();

   /**
    *
    * @param receiverID could be null if the Channel doesn't currently have receivers.
    * @throws Throwable 
    */
   public void storeNACK(Serializable messageID, Serializable receiverID) throws Throwable;

   public boolean forgetNACK(Serializable messageID, Serializable receiverID) throws Throwable;


}
