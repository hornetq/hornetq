/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import java.io.Serializable;

/**
 * A "lightweight representative" of a Message.
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 */
public interface MessageReference extends Routable
{
   /**
    * The Message ID, <i>relative to the MessageStoreImpl</i>
    */
   public Serializable getStorageID();

}
