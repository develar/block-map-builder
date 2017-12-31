# block-map-builder

Generates file block map for differential update using content defined chunking. Robust to insertions, deletions, and changes to input file.

Result written to `stdout`:

```json
{
  // size of input file
  "size": 13423,
  // base64 encoded SHA-512 digest of input-file
  "sha512": "zPFW3WAFUKFvAfBdNXHDIuZekSW/qf33lf5OgKXBKg9oOobwVH9X/DRHExC9087Cxkp3nqFrwtreWZHLso3D6g==",
  // size of block map if appended to input file
  "blockMapSize": 107
}
```

Blockmap is appended to input file if `-out ` not specified.

```
-in string
      input file
-out string
      output file
-avg size
      average chunk size; must be a power of 2 (default 16k)
-compression string
      The compression, one of: gzip, deflate (default "gzip")
-max size
      maximum chunk size (default 32k)
-min size
      minimum chunk size (default 8k)
-window w
      use a rolling hash with window size w (default 64)
``` 