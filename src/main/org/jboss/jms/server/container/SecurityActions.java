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
package org.jboss.jms.server.container;

import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;

import javax.security.auth.Subject;

import org.jboss.security.SecurityAssociation;


/** A collection of privileged actions for this package
 * @author Scott.Stark@jboss.org
 * @author <a href="mailto:alex@jboss.org">Alexey Loubyansky</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:anil.saldhana@jboss.com">anil saldhana</a>
 * @version $Revison: 1.0$
 */
class SecurityActions
{
   interface PrincipalInfoAction
   {
      PrincipalInfoAction PRIVILEGED = new PrincipalInfoAction()
      {
         public void push(final Principal principal, final Object credential,
            final Subject subject)
         {
            AccessController.doPrivileged(
               new PrivilegedAction()
               {
                  public Object run()
                  {
                     SecurityAssociation.pushSubjectContext(subject, principal, credential);
                     return null;
                  }
               }
            );
         }
         public void dup()
         {
            AccessController.doPrivileged(
               new PrivilegedAction()
               {
                  public Object run()
                  {
                     SecurityAssociation.dupSubjectContext();
                     return null;
                  }
               }
            );
         }
         public void pop()
         {
            AccessController.doPrivileged(
               new PrivilegedAction()
               {
                  public Object run()
                  {
                     SecurityAssociation.popSubjectContext();
                     return null;
                  }
               }
            );
         }
      };

      PrincipalInfoAction NON_PRIVILEGED = new PrincipalInfoAction()
      {
         public void push(Principal principal, Object credential, Subject subject)
         {
            SecurityAssociation.pushSubjectContext(subject, principal, credential);
         }
         public void dup()
         {
            SecurityAssociation.dupSubjectContext();
         }
         public void pop()
         {
            SecurityAssociation.popSubjectContext();
         }
      };

      void push(Principal principal, Object credential, Subject subject);
      void dup();
      void pop();
   }

   static void pushSubjectContext(Principal principal, Object credential,
      Subject subject)
   {
      if(System.getSecurityManager() == null)
      {
         PrincipalInfoAction.NON_PRIVILEGED.push(principal, credential, subject);
      }
      else
      {
         PrincipalInfoAction.PRIVILEGED.push(principal, credential, subject);
      }
   }
   
   static void popSubjectContext()
   {
      if(System.getSecurityManager() == null)
      {
         PrincipalInfoAction.NON_PRIVILEGED.pop();
      }
      else
      {
         PrincipalInfoAction.PRIVILEGED.pop();
      }
   }
  }
