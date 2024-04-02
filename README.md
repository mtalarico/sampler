# Sampler

**DISCLAIMER: this is not an official MongoDB product, rather it is experimental code that should be used at your own risk. As with any software, test thoroughly in a lower environment.**

Using statistical sampling based on Cochran's sample size to validate two mongodb cluster's sychronization.

## Compares
- estimated document counts
- indexes
- sample of documents based on statistical analysis

# Sharp Edges
- currently lists all namespaces and keeps them in memory. Except the sampler to blow up on clusters that have a high number of namespaces
- currently compares indexes by name
- currently runs a top level `$or` query to get the batch on the opposite cluster -- even if the collection is not partitioned and only `_id` is used

# TODO:
- [x] remove hard-coded strings from code, replace with consts
- [x] better help output
- [x] sharded collection validation support
- [ ] optimize for `$in` instead of `$or` for collections that just query on `_id`
