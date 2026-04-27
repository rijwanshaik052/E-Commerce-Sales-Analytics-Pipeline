# day 1:
1. we detected new files in incoming folder
- we using path module for handle os and paths
- we use yield module for memory of files
2. for each discovered file, do controlled ingestion checks.
- file scanner ✔
- schema detection ✔
- schema validation ✔
- logging ✔

1. scan files
2. identify file type/schema
3. validate schema
4. validate data
5. move + upload + audit