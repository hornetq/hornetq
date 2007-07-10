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
package org.jboss.jms.server;

import java.util.Set;

import javax.jms.JMSSecurityException;
import javax.security.auth.Subject;

import org.jboss.jms.server.security.SecurityMetadata;
import org.w3c.dom.Element;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface SecurityManager
{
   /**
    * @return the security meta-data for the given destination.
    */
   SecurityMetadata getSecurityMetadata(boolean isQueue, String destName);

   void setSecurityConfig(boolean isQueue, String destName, Element conf) throws Exception;
   void clearSecurityConfig(boolean isQueue, String name) throws Exception;

   /**
    * Authenticate the specified user with the given password. Implementations are most likely to
    * delegates to a JBoss AuthenticationManager.
    *
    * Successful autentication will place a new SubjectContext on thread local, which will be used
    * in the authorization process. However, we need to make sure we clean up thread local
    * immediately after we used the information, otherwise some other people security my be screwed
    * up, on account of thread local security stack being corrupted.
    *
    * @throws JMSSecurityException if the user is not authenticated
    */
   Subject authenticate(String user, String password) throws JMSSecurityException;

   /**
    * Authorize that the subject has at least one of the specified roles. Implementations are most
    * likely to delegates to a JBoss AuthenticationManager.
    *
    * @param rolePrincipals - The set of roles allowed to read/write/create the destination.
    * @return true if the subject is authorized, or false if not.
    */
   boolean authorize(String user, Set rolePrincipals);

}
