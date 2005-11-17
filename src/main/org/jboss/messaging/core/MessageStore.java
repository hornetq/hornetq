/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
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

   boolean isRecoverable();

   /**
    * A non-recoverable message store cannot guarantee recoverability for reliable messages so by
    * default it won't accept reliable messages. If specifically configured to do so, it must
    * unequivocally indicates that it accepts reliable messages by returning true as result of this
    * method.
    *
    * A recoverable message store must always accept reliable messages, so this method must always
    * return true for a recoverable message store.
    *
    * @return false if the channel doesn't accept reliable messages.
    */
   public boolean acceptReliableMessages();


   /**
    * Return a MessageReference instance if already cached. Otherwise create a new one.
    */
   MessageReference reference(Routable r);

   /**
    * Get a pre-existing MessageReference.
    */
   MessageReference getReference(Serializable messageID);
}
