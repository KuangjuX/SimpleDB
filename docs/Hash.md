# 可扩展的 Hash Table

## Split 操作
每次插入超出 bucket 的限制的时候，都需要将 bucket 分离并重新进行 hash。 
考虑到 directory page 对于 hash 值的有效的寻址空间是 global depth 的 2 次幂，因此当 满了的 bucket page 的 local depth 小于 global depth 的时候，
应该首先将 local depth 增加，并且新建一个页，这个页和已经满了的页的元数据应当是相同的。  
  
当新分配一页后应当将已经满了的 bucket page 进行重新 hash，一部分留在当前页中，另一部分将从当前页中移除并插入新分配的页中。
考虑接下来一个例子，假如当前的 global depth 是 3，而已经满了的 bucket page 的 local depth 是 0， 那么他应当被映射到 000, 010, 100, 110 这 4 个 slot 中，
那么当 local depth 增加并创建新的页之后，我们可以设定, 000, 100 的插槽仍然放置原有的页，而 010， 110 则放置新分配的页，并根据 key 的 `hash & GLOBAL_DEPTH_MASK` 的值对之前的 bucket page 的所有 (key, value) 拿出来重新 hash。
那么我们如何将当前的 bucket page 和新分配的 bucket page 进行重新映射呢？ 首先遍历所有的 bucket_idx 并与 LOCAL DEPTH MASK 进行与运算，首先与运算的结果必须同没有分离的时候相同，然后查看高 1 位的值来进行重新映射。
  
当