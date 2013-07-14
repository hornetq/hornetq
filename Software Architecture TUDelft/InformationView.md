#Information View

The _Information View_ describes the way that the architecture stores, manipulates, manages and distributes information. [Rozanski and Woods]

HornetQ contains a high performance journal that handles the presistence aspect.  The designers of HornetQ have applied this solution for performance considerations.  The journal is optimized for the messaging use-cases. If they had used a database it would not provide optimal performance as it is not optimized for this use-case. Concretely, the journal consists of a set of files on the disk. 

The HornetQ core server contains two instances of the journal: 

* Journal instance for Messages
* Journal instance for Bindings

In order to find out what type of data is stored in each type of journal, we can inspect the [JournalStorageManager](https://github.com/hornetq/hornetq/blob/master/hornetq-server/src/main/java/org/hornetq/core/persistence/impl/journal/JournalStorageManager.java), which is the component of the server that interacts with the journal.  In this class, we can find two relevant methods, _loadMessageJournal_ and _loadBindingsJournal_. By reading the body of the methods, we can find in both cases iterations over the whole contents of the journal and inspecting each record for their type. 

Example the from _loadMessageJournal_ method:

 ```java
 switch (recordType)
         {
            case ADD_LARGE_MESSAGE:
            {
               LargeServerMessage largeMessage = parseLargeMessage(messages, buff);

               messages.put(record.id, largeMessage);

               largeMessages.add(largeMessage);

               break;
            }
            case ADD_MESSAGE:
            {
               ServerMessage message = new ServerMessageImpl(record.id, 50);

               message.decode(buff);

               messages.put(record.id, message);

               break;
            }
 ```


In this way we can infer the types of entities that are stored in the journals:

* Message Journal: 
 * Large Message
 * Message
 * Delivery Counts
 * Page Transaction
 * Scheduled Delivery Time
 * Duplicate ID caches
 * Heuristic Completion

* Bindings Journal: 
  * Queues 
  * Queue Attributes
  * Persisted Roles
  * Address Settings
  * ID Sequence Counters
  
![informationview](https://f.cloud.github.com/assets/950121/681081/40f9c360-d9a7-11e2-9ed5-460f54290d68.jpg)

