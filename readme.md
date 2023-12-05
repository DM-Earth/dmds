# dmds

dmds is an asynchronous and multi-dimensional embedded database system.

## Features

### Multi-dimensional

dmds is a multi-dimensional database. It can store data in multiple dimensions. For example, you can store data in the following dimensions:

- `user_id`
- `username_hash`
- `username_length`

In this case, there are 3 dimensions available. This shape the database into a 3D world. See the API document for more information.

With different dimensions, it will be faster to query data with dimensional restrictions.

### Asynchronous

All actions in dmds related with blocking interactions are asynchronous. This means that you can use dmds in a non-blocking way.

### Customize I/O handling

dmds allows you to write your own I/O handling system. This means that you can save data directly in your disk, or save it on another devices through the network.
