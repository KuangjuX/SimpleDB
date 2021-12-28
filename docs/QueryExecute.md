# Query Execution
感觉到了 Lab3 开始比较复杂了，感觉先去整理一下代码会比较好理解一些:  
  
`RID` 表示的是 `Record Identifier`, 由给定的 `page_id` 和 `slot_num` 构成. 当调用 `Get` 方法的时候，他返回一个 64 位的整数, 并返回 `page_id << 32 | slot_num`.  
  
`Tuple` 有 `allocated_`, `rid`, `size_`, `data` 几个域。当初始化 `Tuple` 的时候，它可以根据 `vector<Value>` 和给定的 `Schema` 来初始化。它首先来计算 `Tuple` 的 size，然后根据计算的 size 来分配内存，之后根据输入的每个值对于 `values` 的每个值写入 `data` 域中作序列化。当从 `Tuple` 中获取数据的时候，需要提供 `schema` 和 `column_idx` 从而从 `Tuple` 中获取数据。 
  
`TablePage` 是 `Page` 的具体类，它是在 `Page` 的 `data` 域中具体存储的内容。它的存储格式如下所示:
```
Slotted page format:
---------------------------------------------------------
| HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
---------------------------------------------------------
```
其中 `Header` 是从前向后，`Tuple` 是从后向前，当 `Header` 与 `Tuple` 相遇时表示页已经被用完了。  
  
`Header` 的格式如下所示:
```
Header format (size in bytes):
----------------------------------------------------------------------------
| PageId (4)| LSN (4)| PrevPageId (4)| NextPageId (4)| FreeSpacePointer(4) |
----------------------------------------------------------------------------
----------------------------------------------------------------
| TupleCount (4) | Tuple_1 offset (4) | Tuple_1 size (4) | ... |
----------------------------------------------------------------
```  
当从 `TablePage` 中获取 `Tuple` 的时候，首先找到对应 `slot_idx` 的 `tuple_size` 判断其是否被删除。如果没有被删除。我们从 `Header` 中找到它对应的 `Tuple` 的 offset。然后把对应偏移量的字节拷贝到要返回的 tuple 中并返回。  

`TableHeap` 则是对于一张表的所有页的管理者，它拥有 `buffer_pool_manager` 可以获取对应的页，`first_page_id` 表示的该表的第一页的 id。通过 `Begin` 和 `End` 方法可以获得表里第一个元组和最后一个元组的迭代器。 
  
`TableIterator` 是对于一张表里每个元组的迭代器，它拥有 `table_heap_`，`tuple`, `txn` 三个成员变量，它的构造函数可以通过 `RID` 来获取对应的 `Tuple`。同时 `TableIterator` 也重载了操作符，当调用 `++` 的时候它会找到下一条 `Tuple` 并返回一个 `TableIterator` 对象。