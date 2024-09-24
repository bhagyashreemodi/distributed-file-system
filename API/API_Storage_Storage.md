# Storage Server API Specification - Storage Interface

Clients (and other storage servers) use this interface to access files hosted by a storage
server. This interface will be created using the localhost/127.0.0.1 server address and the port number
included in the `storageNCommand` strings defined in `test/ServerCommands.java`.

If the storage server cannot parse a received command, it should respond with `400 Bad Request`.

------

## `/storage_size` Command

**Description**: Clients use this command to request the length of a file in bytes.

### Request from client

**Command**: `/storage_size`

**Method**: `POST`

**Input Data**:
```json
{
    "path": "/path/to/file"
}
```

* *path*: The path string to the file of interest.

A sample Java class representing this command can be found at `common/PathRequest.java`.

### Response to client

**Code**: `200 OK`

**Content**:
```json
{
    "size": 1024
}
```

* *size*: the length of the file in bytes.

A sample Java class representing this response can be found at `common/SizeReturn.java`.

### Error response to client

**Code**: `404 Not Found`

**Content**:
```json
{
    "exception_type": "FileNotFoundException",
    "exception_info": "the parent directory does not exist."
}
```

* *exception_type*: can be `FileNotFoundException` if the file does not exist (or is a directory) or `IllegalArgumentException` if the path is otherwise invalid
* *exception_info*: you can put whatever information is useful for your own debugging purposes.

A sample Java class representing this response can be found at `common/ExceptionReturn.java`

------

## `/storage_read` Command

**Description**: Clients use this command to read a sequence of bytes from a file.

### Request from client

**Command**: `/storage_read`

**Method**: `POST`

**Input Data**:
```json
{
    "path": "/path/to/file"
    "offset": 2222,
    "length": 3333
}
```

* *path*: The path string to the file of interest.
* *offset*: Position within the file to start reading.
* *length*: The number of bytes to read.

A sample Java class representing this command can be found at `common/ReadRequest.java`.

### Response to client

**Code**: `200 OK`

**Content**:
```json
{
    "data": "kaljsdbojackhorsemanklajemke"
}
```

* *data*: Base64 encoding of the bytes read from the file, as JSON doesn't support byte arrays. A successful call will return the number of bytes that were requested.

A sample Java class representing this response can be found at `common/DataReturn.java`.

### Error response to client

**Code**: `404 Not Found`

**Content**:
```json
{
    "exception_type": "FileNotFoundException",
    "exception_info": "File not found on storage server"
}
```

* *exception_type*: 
    * `FileNotFoundException` if the file cannot be found or the path refers to a directory
    * `IndexOutOfBoundsException` if the sequence specified by `offset` and `length` goes outside the bounds of the file, or if `length` is negative
    * `IOException` if the file read cannot be completed on the server
    * `IllegalArgumentException` if the path is invalid
* *exception_info*: you can put whatever information is useful for your own debugging purposes.

A sample Java class representing this response can be found at `common/ExceptionReturn.java`

------

## `/storage_write` Command

**Description**: Clients use this command to write a sequence of bytes to a file.

### Request from client

**Command**: `/storage_write`

**Method**: `POST`

**Input Data**:
```json
{
    "path": "/path/to/file"
    "offset": 2222,
    "data": "kljasdarickandmortyaklsdea"
}
```

* *path*: The path string to the file of interest.
* *offset*: Position within the file to start writing.
* *data*: Base64 encoding of the bytes to write into the file.

A sample Java class representing this command can be found at `common/WriteRequest.java`.

### Response to client

**Code**: `200 OK`

**Content**:
```json
{
    "success": true
}
```

* *success*: boolean value indicating whether the file was successfully written.

A sample Java class representing this response can be found at `common/BooleanReturn.java`.

### Error response to client

**Code**: `404 Not Found`

**Content**:
```json
{
    "exception_type": "FileNotFoundException",
    "exception_info": "File not found on storage server"
}
```

* *exception_type*: 
    * `FileNotFoundException` if the file cannot be found or the path refers to a directory
    * `IndexOutOfBoundsException` if the `offset` is negative
    * `IOException` if the file write cannot be completed on the server
    * `IllegalArgumentException` if the path is invalid
* *exception_info*: you can put whatever information is useful for your own debugging purposes.

A sample Java class representing this response can be found at `common/ExceptionReturn.java`

