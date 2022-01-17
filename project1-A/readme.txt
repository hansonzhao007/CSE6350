# input sample

`input_sample.txt`: This file includes the sample inputs for your key-value store. The first line is the decripter of columns.

`input.txt`: This is the for your testing.

descripter: type,key1,key2,value

columns:
- type: There are four types of operations. `put`, `get`, `scan` and `del`.
- key1: All operation types have content in this column. It is a 16-byte string.
- key2: Only `scan` type has content in this column. It is a 16-byte string.
- value: Only `put` type has content in this column. It is a 16-byte string.

# output sample

'output_sample.txt`: THis is the example outputs from the `input_sample.txt`.

descripter: type,key1,outcome,values

columns:
- type: Type of request, should be same with the type in input file
- key1: same in input file
- outcome:
    0: key exsits in kv store
    1: key does not exsit in kv store
- values:
    put: none
    get: the value of the key
    scan: the values within the [key1, key2]
    del: none

# Note

For this project, you only have to implement put/get/del.

