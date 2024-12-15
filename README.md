Hoosat Network Database Filler

The Hoosat Network database filler is the tool to move information from the HTN blockchain to PostgreSQL database. It has been extensively modified from IAm3R's original code. Like batch processing to avoid crashing the db filler on big transaction blocks. 

**There are few optional Environment values:**
  
1. Enabling transaction batch processing:
```
BATCH_PROCESSING=true
```

2. Starting point of processing:
```
START_HASH=HASHVALUE
```
**Remark** HTN database filler is one of the first database fillers that can crawl the blockchain from genesis to the database.

3. Enable balance processing:
```
BALANCE_ENABLED=true
```

4. Enable updating balances from the database when filler is started:
```
UPDATE_BALANCE_ON_BOOT=true
```

**Modifications**
1. Fix big transaction processing.
2. Add batch processing of transactions.
3. Add starting point for processing. 
4. Add balance processing. 
