# wikitools

tools for extracting data from Wikipedia dumps

## Todo

- [ ] Clean input data
   + [ ] Remove punctuation
- [ ] Replace all redirectable pages with their respective redirects
- [ ] Use page-to-page-links dump as source of mutual-outlink index
   + [ ] Parse and load page-to-page MySQL dump
   + [ ] Use page-to-page links store, not page anchor data, during Tantivy
      index construction

## References

```
@inproceedings{Hasibi:2016:ORT, 
   author =    {Hasibi, Faegheh and Balog, Krisztian and Bratsberg, Svein Erik},
   title =     {On the reproducibility of the TAGME Entity Linking System},
   booktitle = {roceedings of 38th European Conference on Information Retrieval},
   series =    {ECIR '16},
   year =      {2016},
   pages =     {436--449},
   publisher = {Springer},
   DOI =       {http://dx.doi.org/10.1007/978-3-319-30671-1_32}
} 
```