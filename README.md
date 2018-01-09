# NotNaive_RDBMS
A relational database system in C++. Indexing, join, select, project. 
Supports - int, float and varchar data types


## Storing records 
1) The basic unit of data that is written/read from disk is a page of size 4096 bytes (4 KB).
2) All records are stored in an unordered heap-file structure.
3) Every record has a unique record id, which is a tuple consisting of the page number and the serial no. of the record in this page.
4) Field access occurs in O(1) unit time. 

The page structure is as follows:<br>
[record1, record2, ..., record N, xx-freespace-xx, slot-table]

The record structure is as follows:<br>
[version, null-byte array, field-offsets array, field1, field2, field3, ...]

<li>Version can be used for schema versioning, or to indicate if the record is a tombstone (record was moved to other page)</li>
<li>Null-byte array indicates which field are NULL</li>
<li>Field-offsets array contains offsets of fields relative to the start of the record </li>

<br>
For int and float, 4 bytes are used.
For varchar, 4 bytes are used to store length of characters, then 1-byte per character.
<br>

## Insert, Delete, Update
Insert: The db-file is searched for a page, such that it has enough space to accomodate the record.
<br><br>
Delete: The corresponding record is deleted from the page. Subsequent records are moved to "the left", so that there aren't any gaps between records. 
<br><br>
Update: First it is checked if the same page can accomadate the updated record. If yes, then the record is modified in place, and subsequent records are moved to "the left" or to "the right". If there is not enough space, the record is moved to another page and a <b>tombstone</b> is left in the original location. This tombstone indicates the RID: pagenumber, slotnum of the record in a new page.
<br><br>

## Indexing 
B+ Tree indexes can be created on any attribute(s), for any data-type. 
The non-leaf nodes of the tree contain 'pointers' to go to the appropriate leaf nodes. The leaf nodes contain the actual key values and the corresponding RIDs for that record. <br><br>
So, record retrieval works in the following way:<br>
<li>Navigate to the appropriate leaf node</li>
<li> Search that leaf node for exact key, and fetch the RID from that key,rid tuple</li>
<li> Fetch actual record from the heap-file</li>

<br><b>P.S. Range selects/queries are also supported </b><br><br><br>
The following diagram depicts the indexing structure:<br>
![alt text](https://github.com/pushkarmoi/NotNaive_RDBMS/blob/master/tree.png)

## Selection, Projection, Aggregate operations (min, max, sum, average)

These operators are applied 'on-the-fly' to records.

## Joins (Block-Nested loop and Index-nested loop)



