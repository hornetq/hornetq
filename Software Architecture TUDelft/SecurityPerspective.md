#Security Perspective

In their [book](http://www.viewpoints-and-perspectives.info/), Rozanski and Woods describe the security perspective as being "the ability of the system to reliably control, monitor, and audit who can perform what actions on these resources and the ability to detect and recover from failures in security mechanisms".  In the following sections we will apply this definition in the context of HornetQ.

###Role-based security model

In HornetQ, the security aspect is implemented as a form of [role-based access control](http://csrc.nist.gov/groups/SNS/rbac/documents/ferraiolo-kuhn-92.pdf). In this model, users that have a user-name and a password are assigned roles, which define which type of operations they are allowed to carry out and which not. This decoupling between individual users and the possible tasks simplifies the user management.  For example, it is easier to add a user and assign roles rather than a list of tasks. 

### Security for queues

In order to understand which are the entities that are "guarded" by the role-based access control of HornetQ, we have to first go back to how are queues handled in the HornetQ server. Multiple queues can be bound to one address.  Incomming messages have that address as destination.  After the incoming message is received by the server, it delivers the message to all queues that are registered under that address. 

In HornetQ, the security aspect is related to the addresses.  For these, we can define types of actions and the correspondent roles that have the priviledge to carry out those actions. Firstly, the actions: 

* createDurableQueue - create a durable queue under matching addresses.
* deleteDurableQueue - delete a durable queue under matching addresses.
* createNonDurableQueue - create a non-durable queue under matching addresses.
* deleteNonDurableQueue - delete a non-durable queue under matching addresses.
* send - send a message to matching addresses.
* consume -  consume a message from a queue bound to matching addresses.
* manage - invoke management operations by sending management messages to the management address.

These actions can be assigned to specific roles in the hornetq-configuration.xml file.  An example of such an assignment is provided below:

```xml
<security-setting match="globalqueues.europe">
    <permission type="createDurableQueue" roles="admin"/>
    <permission type="deleteDurableQueue" roles="admin"/>
    <permission type="createNonDurableQueue" roles="admin, guest, europe-users"/>
    <permission type="deleteNonDurableQueue" roles="admin, guest, europe-users"/>
    <permission type="send" roles="admin, europe-users"/>
    <permission type="consume" roles="admin, europe-users"/>
</security-setting>     
```
In the snippet above, only the admin role is given priviledges to create/delete durable queues. Guests can only create non-durable queues. 

###User credentials

The mapping between user accounts (username and password) and the assigned roles is defined in the hornetq-users.xml file which is read by the security manager. An example of such a file is presented below: 

```xml
<configuration xmlns="urn:hornetq" 
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="urn:hornetq ../schemas/hornetq-users.xsd ">
    
    <defaultuser name="guest" password="guest">
        <role name="guest"/>
    </defaultuser>
    
    <user name="tim" password="marmite">
        <role name="admin"/>      
    </user>
    
    <user name="andy" password="doner_kebab">
        <role name="admin"/>
        <role name="guest"/>
    </user>
    
    <user name="jeff" password="camembert">
        <role name="europe-users"/>
        <role name="guest"/>
    </user>
    
</configuration>
```


The defaultuser is for the situations when the user does not provide a user/password pair when creating a new session.  The follow 3 users which have various roles and therefore, various levels of restriction concerning the possible actions they can undertake in the server.  

###Default Security Manager Implementation

By default, HornetQ ships with a default Security Mangager embodied in the [HornetQSecurityManagerImpl](https://github.com/hornetq/hornetq/blob/master/hornetq-server/src/main/java/org/hornetq/spi/core/security/HornetQSecurityManagerImpl.java) class.  Basically it contains HashMaps for user-role assignments and role-actions assignments.  The interface it implements, HornetQSecurityManager provides methods for user manipulation (adding, deleting) and validations (validate user and validate both user and role).  

This default Security Manager can be replaced by other implementations that adhere to the mentioned interface.  The point where new security managers can be hooked is the hornetq-beans.xml file, in the following way (example is for the default security manager): 

```xml
<bean name="HornetQSecurityManager" 
      class="org.hornetq.spi.core.security.HornetQSecurityManagerImpl">
    <start ignored="true"/>
    <stop ignored="true"/>
</bean> 
```
#References

[1] HornetQ Security in User Manual - http://docs.jboss.org/hornetq/2.2.5.Final/user-manual/en/html/security.html - accessed 20 June 2013

