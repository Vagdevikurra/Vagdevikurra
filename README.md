**Big problem found.** Only transmit rows came back (wealth returned 0 — probably the date didn't match exactly). But look at those transmit `rcif_id` values:

- `0000000708701` — 13 digits, leading zeros
- `1800546665000` — 13 digits, **trailing zeros**
- `1907354981000` — 13 digits, trailing zeros

**These are NOT RCIF numbers.** RCIFs are 7-9 digits. These look like phone numbers or opaque IDs. The join has been matching nothing which is why active counts are only ~10K instead of ~65K.

Run these two separately real quick:

```sql
-- Wealth RCIF format
SELECT CAST(rcif_cust_nbr AS STRING) AS rcif
FROM eil.d_involved_party_h
WHERE source_system_code = 'CF'
LIMIT 5
```

```sql
-- What columns does transmit actually have?
SELECT rcif_id, opaque_id, profile_id
FROM dm_ib.transmit_digital_logins
LIMIT 5
```

The second query will show us all three ID columns — one of them will match the RCIF format. My guess is `profile_id` or `opaque_id` is the real RCIF, not `rcif_id`.
