# Sampler

**DISCLAIMER: this is not an official MongoDB product, rather it is experimental code that should be used at your own risk. As with any software, test thoroughly in a lower environment.**

WIP statistical sampling to validate two mongodb cluster's sychronization.

Compares
- estimated document counts
- indexes
- sample of documents based on statistical analysis

TODO:
- [x] remove hard-coded strings from code, replace with consts
- [x] better help output
- [x] sharded collection validation support
