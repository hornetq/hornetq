/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.contract;


/**
 * When loading a message from storage, references from different channels can reference the same message.
 * In order to avoid loading the message more than once, loaded or paged references are stored in the message store
 * 
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</ttH>
 *
 * $Id$
 */
public interface MessageStore extends MessagingComponent
{
	 MessageReference reference(long messageID);
	 
	 MessageReference reference(Message message);
}
