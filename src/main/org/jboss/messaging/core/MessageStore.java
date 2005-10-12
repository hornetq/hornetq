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
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface MessageStore
{
   Serializable getStoreID();

   boolean isReliable();

   /**
    * Return a MessageReference instance if already cached.
    * Otherwise create a new one
    * 
    * @param r
    * @return
    */
   MessageReference reference(Routable r);

   /**
    *  Get a pre-existing MessageReference
    *  
    * @param messageID
    * @return
    */
   MessageReference getReference(Serializable messageID);
   
   

}
