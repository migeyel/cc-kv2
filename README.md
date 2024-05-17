Hi. The code is undergoing some major changes and doesn't really work right now. Go
check out [cb7a85a](https://github.com/migeyel/cc-kv2/tree/cb7a85a723de108976db95cbad3e89209382acf7)
if you want code that works.

# `cc-kv2`
`cc-kv2` is a WIP transactional ordered key-value store running on ComputerCraft
and craftos-pc.

Transactions are meant to be ACID compliant, assuming `fs` behaves like Linux,
except for restoring from a world backup, power outage, or kernel panic. The
isolation level is serializable, but there is no deadlock detection.

### Stability
As of right now, running out of space will likely put the database in an
unrecoverable state because recovery requires writing extra data to the log.
There are very few tests, so you shouldn't expect stored data to remain
uncorrupted for long. Furthermore, the disk data structures are expected to
change throughout development until a stable release is made.

### Features not yet Implemented
- A way to keep the data intact when restoring from a backup.
- Deadlock detection and recovery.
- Multiple disk spanning storage.
- Maximum log size and automatic out-of-space rollbacks.

### Building
1. Clone the repository. 
2. `npm install`
3. `npm run build`

The output module is at `main.lua`.

### Usage
```lua
-- Open the database.
local cckv2 = require "cckv2"
local db = cckv2.open("/db")

-- Start a new transaction.
local tx = db:begin()

-- Set values.
tx:set("foo", "1")
tx:set("bar", "2")

-- Get values.
print(tx:get("foo")) -- "1"
print(tx:get("bar")) -- "2"

-- Find the next value in lexicographic order.
print(tx:next())      -- "bar", "2"
print(tx:next("bar")) -- "foo", "1"
print(tx:next("boo")) -- "foo", "1"
print(tx:next("foo")) -- nil

-- Iterate through values.
for k, v in tx:iter() do print(k, v) end
for k, v in tx:iter("bar") do print(k, v) end

-- Delete values.
tx:delete("foo")

-- Commit the transaction.
tx:commit()

-- Release the database lock.
-- Not calling this will throw an error when trying to open again.
-- If you encounter that error, reboot the computer.
db:close()
```
