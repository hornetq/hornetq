/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

import java.io.Serializable;

/**
 * A message store is a transactional and reliable repository for messages. It physically stores
 * reliable messages and generates references.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface MessageStore
{
   Serializable getStoreID();

   boolean isReliable();

   /**
    * Store the message reliabily. However, if the message is unreliable (does not need
    * recoverabilitity), it caches the message in memory and returns a reference.
    *
    * @exception Throwable - thrown in case of storage failure.
    */
   MessageReference reference(Routable r) throws Throwable;

   MessageReference getReference(Serializable messageID);

}
