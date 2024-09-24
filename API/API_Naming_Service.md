# Naming Server API Specification - Service Interface

Clients use this interface to interact with the naming server. This interface will be created 
using the localhost/127.0.0.1 server address and the port number included in the `namingCommand` 
string defined in `test/ServerCommands.java`.

If the naming server cannot parse a received command, it should respond with `400 Bad Request`.

------

## `/is_valid_path` Command

**Description**: A client uses this command to determine whether a path is valid. The path string
must be a sequence of components beginning with and delimited by forward slashes, not including 
any spaces or colons, e.g., `/dir/file`

### Request from client

**Command**: `/is_valid_path`

**Method**: `POST`

**Input Data**:
```json
{
    "path": "/path/to/file"
}
```

* *path*: The path string described as above.

A sample Java class representing this command can be found at `common/PathRequest.java`.

### Response to client

**Code**: `200 OK`

**Content**:
```json
{
    "success": true
}
```

* *success*: boolean value indicating whether the given path is valid (`true`) or invalid (`false`).

A sample Java class representing this response can be found at `common/BooleanReturn.java`.

------

## `/get_storage` Command

**Description**: A client uses this command to request IP address and port number of a storage server
hosting the file indicated by the path string. If the client intends to perform `read` or `size` commands,
it should lock the file for shared access before making this call; if it intends to perform a `write`
command, it should lock the file for exclusive access.

### Request from client

**Command**: `/get_storage`

**Method**: `POST`

**Input Data**:
```json
{
    "path": "/path/to/file"
}
```

* *path*: string containing the path to the file

A sample Java class representing this command can be found at `common/PathRequest.java`.

### Successful response to client

**Code**: `200 OK`

**Content**:
```json
{
    "server_ip": "localhost",
    "server_port": 1111
}
```

* *server_ip*: IP address of a storage server hosting the file
* *server_port*: client access port of the storage server hosting the file

A sample Java class representing this command can be found at `common/ServerInfo.java`.

### Error response to client

**Code**: `404 Not Found`

**Content**:
```json
{
    "exception_type": "FileNotFoundException",
    "exception_info": "File/path cannot be found."
}
```

* *exception_type*: can be `FileNotFoundException` if the file is not present in the file system or `IllegalArgumentException` if the path is otherwise invalid
* *exception_info*: you can put whatever information is useful for your own debugging purposes.

A sample Java class representing this response can be found at `common/ExceptionReturn.java`

------

## `/delete` Command

**Description**: A client uses this command to request a file/directory to be deleted from the file system. The
parent directory of the file/directory should be locked for exclusive access before this operation is performed.

### Request from client

**Command**: `/delete`

**Method**: `POST`

**Input Data**:
```json
{
    "path": "/path/to/a/file/or/dir"
}
```

* *path*: string containing the path to the file or directory to be deleted

A sample Java class representing this command can be found at `common/PathRequest.java`.

### Successful response to client

**Code**: `200 OK`

**Content**:
```json
{
    "success": true
}
```

* *success*: boolean value indicating whether the requested file/directory is successfuly deleted (note: the root directory cannot be deleted)

A sample Java class representing this response can be found at `common/BooleanReturn.java`.

### Error response to client

**Code**: `404 Not Found`

**Content**:
```json
{
    "exception_type": "FileNotFoundException",
    "exception_info": "the file/directory or parent directory does not exist."
}
```

* *exception_type*: can be `FileNotFoundException` if the file/directory or its parent does not exist or `IllegalArgumentException` if the path is otherwise invalid
* *exception_info*: you can put whatever information is useful for your own debugging purposes.

A sample Java class representing this response can be found at `common/ExceptionReturn.java`

------

## `/create_directory` Command

**Description**: A client uses this command to create a new directory within the file system, 
if it does not exist already. The parent directory of the new directory should be locked for 
exclusive access before this operation is performed.

### Request from client

**Command**: `/create_directory`

**Method**: `POST`

**Input Data**:
```json
{
    "path": "/path/to/dir"
}
```

* *path*: string containing the path to the desired new directory to be created

A sample Java class representing this command can be found at `common/PathRequest.java`.

### Successful response to client

**Code**: `200 OK`

**Content**:
```json
{
    "success": true
}
```

* *success*: boolean value indicating whether the requested directory is successfuly created, which is `false` if a file or directory with the given name already exists

A sample Java class representing this response can be found at `common/BooleanReturn.java`.

### Error response to client

**Code**: `404 Not Found`

**Content**:
```json
{
    "exception_type": "FileNotFoundException",
    "exception_info": "the parent directory does not exist."
}
```

* *exception_type*: can be `FileNotFoundException` if the parent directory does not exist or `IllegalArgumentException` if the path is otherwise invalid
* *exception_info*: you can put whatever information is useful for your own debugging purposes.

A sample Java class representing this response can be found at `common/ExceptionReturn.java`

------

## `/create_file` Command

**Description**: A client uses this command to create a new file within the file system, 
if it does not exist already. The parent directory of the new file should be locked for 
exclusive access before this operation is performed.

### Request from client

**Command**: `/create_file`

**Method**: `POST`

**Input Data**:
```json
{
    "path": "/path/to/file"
}
```

* *path*: string containing the path to the desired new file to be created

A sample Java class representing this command can be found at `common/PathRequest.java`.

### Successful response to client

**Code**: `200 OK`

**Content**:
```json
{
    "success": true
}
```

* *success*: boolean value indicating whether the requested file is successfuly created, which is `false` if a file or directory with the given name already exists

A sample Java class representing this response can be found at `common/BooleanReturn.java`.

### Error response to client -- parent directory doesn't exist or invalid path given

**Code**: `404 Not Found`

**Content**:
```json
{
    "exception_type": "FileNotFoundException",
    "exception_info": "the parent directory does not exist."
}
```

* *exception_type*: can be `FileNotFoundException` if the parent directory does not exist or `IllegalArgumentException` if the path is otherwise invalid
* *exception_info*: you can put whatever information is useful for your own debugging purposes.

A sample Java class representing this response can be found at `common/ExceptionReturn.java`

### Error response to client -- no storage servers registered

**Code**: `409 Conflict`

**Content**:
```json
{
    "exception_type": "IllegalStateException",
    "exception_info": "no storage servers are registered with the naming server."
}
```

* *exception_type*: `IllegalStateException` if there is no registered storage server to store the requested file
* *exception_info*: you can put whatever information is useful for your own debugging purposes.

A sample Java class representing this response can be found at `common/ExceptionReturn.java`


------

## `/list` Command

**Description**: A client uses this command to retrieve a list of contents within a given directory.
The directory should be locked for shared access before this operation is performed, to allow for
safe reading of the directory contents.

### Request from client

**Command**: `/list`

**Method**: `POST`

**Input Data**:
```json
{
    "path": "/path/to/dir"
}
```

* *path*: string containing the path to the directory of interest

A sample Java class representing this command can be found at `common/PathRequest.java`.

### Successful response to client

**Code**: `200 OK`

**Content**:
```json
{
    "files": [
        "file1",
        "file2",
        "file3"
    ]
}
```

* *files*: a list/array of strings, which are not guaranteed to be in any particular order.

A sample Java class representing this command can be found at `common/FilesReturn.java`.

### Error response to client -- directory doesn't exist or invalid path given

**Code**: `404 Not Found`

**Content**:
```json
{
    "exception_type": "FileNotFoundException",
    "exception_info": "the directory does not exist."
}
```

* *exception_type*: can be `FileNotFoundException` if the directory does not exist or `IllegalArgumentException` if the path is otherwise invalid
* *exception_info*: you can put whatever information is useful for your own debugging purposes.

A sample Java class representing this response can be found at `common/ExceptionReturn.java`

------

## `/is_directory` Command

**Description**: A client uses this command to determine whether a path refers to a directory. 
The parent directory should be locked for shared access before this operation is performed, to 
prevent the file/directory in question from being deleted or created while this call is in progress.

### Request from client

**Command**: `/is_directory`

**Method**: `POST`

**Input Data**:
```json
{
    "path": "/path/to/be/checked"
}
```

* *path*: string containing the path to check

A sample Java class representing this command can be found at `common/PathRequest.java`.

### Successful response to client

**Code**: `200 OK`

**Content**:
```json
{
    "success": true
}
```

* *success*: boolean value indicating whether the path points to a directory (`true`) or a file (`false`)

A sample Java class representing this response can be found at `common/BooleanReturn.java`.

### Error response to client

**Code**: `404 Not Found`

**Content**:
```json
{
    "exception_type": "FileNotFoundException",
    "exception_info": "the file/directory or parent directory does not exist."
}
```

* *exception_type*: can be `FileNotFoundException` if the file/directory does not exist or `IllegalArgumentException` if the path is otherwise invalid
* *exception_info*: you can put whatever information is useful for your own debugging purposes.

A sample Java class representing this response can be found at `common/ExceptionReturn.java`

------

## `/unlock` Command

**Description**: A client uses this command to unlock a file/directory that it previously locked.

### Request from client

**Command**: `/unlock`

**Method**: `POST`

**Input Data**:
```json
{
    "path": "/path/to/file/or/dir",
    "exclusive": true
}
```

* *path*: string containing the path to the file/directory to be unlocked
* *exclusive*: must be `true` if the object was locked for exclusive access and `false` if it was locked for shared access

A sample Java class representing this command can be found at `common/LockRequest.java`.

### Successful response to client

**Code**: `200 OK`

**Content**: empty

### Error response to client

**Code**: `404 Not Found`

**Content**:
```json
{
    "exception_type": "IllegalArgumentException",
    "exception_info": "the file/directory cannot be found"
}
```

* *exception_type*: `IllegalArgumentException` if the file/directory is not locked by this client or the path is otherwise invalid
* *exception_info*: you can put whatever information is useful for your own debugging purposes.

A sample Java class representing this response can be found at `common/ExceptionReturn.java`

------

## `/lock` Command

**Description**: A client uses this command to lock a file/directory for either shared or
exclusive access.  An **exclusive** lock prevents any other client from locking the object for
any use until it is unlocked; such locks are typically used for modifying object state. A 
**shared** lock allows other clients to obtain shared locks for the same object at the same 
time but not an exclusive lock; such locks are typically used for accessing object state while
ensuring no other client modifies it.

Exclusive locks are safer than shared locks and can be used whenever objects are accessed. However
this blocks access to all other clients and reduces system throughput if objects are popular.

The naming server considers each shared lock to be a read request, and it counts such lock calls
as the basis for making replication decisions.  Locking a file for exclusive access is considered
as a write request, which may lead to invalidation/deletion of stale replicas.  The naming server
must treat all lock actions as read or write requests because it cannot monitor the true read and
write actions that take place directly between clients and storage servers.

When any directory/file is locked for either kind of access, all directories along the path up to, but
not including, the target directory/file itself are locked for shared access to prevent their 
modification or deletion by other clients.  For example, if one client locks `/etc/scripts/startup.sh`
for exclusive access to update the script, then `/`, `/etc`, and `/etc/scripts` will all be locked
for shared access to prevent conflicts.

A directory/file can be considered to be **effectively locked** for exclusive access if any of the
directories on its path is already locked for exclusive access -- this is because no client will be
able to obtain any kind of lock until the "blocking" exclusive lock is released.  This is a direct
consequence of the locking order described above.  As such, if a directory is locked for exclusive
access, the entire subtree under that directory can also be considered to be locked for exclusive
access.  If a client takes advantage of this fact to lock a directory and then perform several
reads and/or writes under it, the naming server may lose track of file state; if a client locks a
directory and then writes to files in the directory, the naming server may not know that other replicas
of these files need to be invalidated or updated.  This is a limitation of this file system design.

A minimal amount of fairness is guaranteed with locking.  Clients are served in a first-come first-served
order, with a slight modification.  If multiple clients request shared access of the same object, these
locks can be granted simultaneously.  However, if any exclusive lock request is waiting for the lock on
the object, no additional shared locks should be granted, otherwise the exclusive lock request may
wait forever, leading to starvation.  Instead, any shared lock requests that arrive after an exclusive
lock request should wait until the exclusive lock is granted and released.  For example, suppose clients
`A` and `B` currently hold a shared lock on a file, and `C` has requested exclusive access to the same
file.  If another client `D` requests shared access to the file, this request should be queued until both
`A` and `B` release the shared lock, `C` is granted the exclusive lock, and `C` releases the exclusive
lock.

### Request from client

**Command**: `/lock`

**Method**: `POST`

**Input Data**:
```json
{
    "path": "/path/to/file/or/dir",
    "exclusive": true
}
```

* *path*: string containing the path to the file/directory to be locked
* *exclusive*: `true` for requesting exclusive access or `false` for shared access

A sample Java class representing this command can be found at `common/LockRequest.java`.

### Successful response to client

**Code**: `200 OK`

**Content**: empty (no response needed for successful lock, as the OK grants access itself)

### Error response to client

**Code**: `404 Not Found`

**Content**:
```json
{
    "exception_type": "FileNotFoundException",
    "exception_info": "the file/directory cannot be found"
}
```

* *exception_type*: can be `FileNotFoundException` if the file/directory does not exist or `IllegalArgumentException` if the path is otherwise invalid
* *exception_info*: you can put whatever information is useful for your own debugging purposes.

A sample Java class representing this response can be found at `common/ExceptionReturn.java`

