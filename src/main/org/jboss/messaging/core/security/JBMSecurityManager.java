package org.jboss.messaging.core.security;

import java.util.Set;

/**
 * USe to validate whether a user has is valid to connect to the server and perform certain functions
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface JBMSecurityManager
{
   /**
    * is this a valid user.
    * @param user the user
    * @param password the users password
    * @return true if a valid user
    */
   boolean validateUser(String user, String password);

   /**
    * is this a valid user and do they have the correct role
    *
    * @param user the user
    * @param password the users password
    * @param roles the roles the user has
    * @param checkType the type of check to perform
    * @return true if the user is valid and they have the correct roles
    */
   boolean validateUserAndRole(String user, String password, Set<Role> roles, CheckType checkType);
}
