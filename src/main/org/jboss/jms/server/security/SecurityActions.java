/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.security;

import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;

import javax.security.auth.Subject;

import org.jboss.security.SecurityAssociation;


/** A collection of privileged actions for this package
 * @author Scott.Stark@jboss.org
 * @author <a href="mailto:alex@jboss.org">Alexey Loubyansky</a>
 * @author <a her="mailto:tim.fox@jboss.com">Tim Fox</a>
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
  }
