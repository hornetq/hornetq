/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.interfaces;

import java.io.Serializable;
import java.util.Set;

/**
 * An atomic, self containted unit of data that flows through the system.
 *
 * It supports the concept of message header. Various messaging system components can attach or
 * remove headers to/from the message, primarily for message flow management purposes.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface Message extends Serializable, Cloneable
{
   public static final String REMOTE_MESSAGE_HEADER = "REMOTE_MESSAGE_HEADER";
   public static final String REPLICATOR_ID_HEADER = "REPLICATOR_ID_HEADER";

   public Serializable getMessageID();

   public void putHeader(String name, Serializable value);

   public Serializable getHeader(String name);

   public Serializable removeHeader(String name);

   public Set getHeaderNames();

   /**
    * Make sure that classes implementing this interface propery override clone() method.
    */
   public Object clone();
   
}
