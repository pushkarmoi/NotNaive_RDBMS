# NotNaive_RDBMS
A relational database system in C++. Indexing, join, select, project. 
Supports - int, float and varchar data types


## Storing records 
1) The basic unit of data that is written/read from disk is a page of size 4096 bytes (4 KB).
2) All records are stored in an unordered heap-file structure.
3) Every record has a unique record id, which is a tuple consisting of the page number and the serial no. of the record in this page.
4) Field access occurs in O(1) unit time. 

The record structure is as follows:
version, null-byte array, field-offsets array, field1, field2, field3, ...

-> Version can be used for schema versioning, or to indicate if the record is a tombstone (record was moved to other page)
-> null-byte array indicates which field are NULL.
-> field-offsets array contains offsets of fields relative to the start of the record.

For int and float, 4 bytes are used.
For varchar, 4 bytes are used to store length of characters, then 1-byte per character.

