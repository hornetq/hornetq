package org.jboss.messaging.core.security;

/**
 * extends JBMSecurityManager to allow the addition and removal of users and roles.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface JBMUpdateableSecurityManager extends JBMSecurityManager
{
   /**
    * adds a new user
    * @param user the user to add
    * @param password theusers password
    */
   void addUser(String user, String password);

   /**
    * removes a user and any roles they may have.
    * @param user the user to remove
    */
   void removeUser(String user);

   /**
    * adds a new role for a user.
    * @param user the user
    * @param role the role to add
    */
   void addRole(String user, String role);

   /**
    * removes a role from a user
    * @param user the user
    * @param role the role to remove
    */
   void removeRole(String user, String role);
}
