# ID Differences: UI Download vs API Download
**Issue:** Different IDs in UI-downloaded CSV vs API-downloaded CSV  
**Date:** 2025-11-28
---
## The Difference Explained
### UI Download
**Columns:**
- `Tweet ID` - Twitter/X native Snowflake ID (e.g., `1994149756910883262`)
- `User` - Twitter handle as string (e.g., `elonmusk`)
### API Download
**Columns:**
- `id` - **Polymarket internal database ID** (CUID format, e.g., `cmiip5awe0000i004bhwnpcnb`)
- `userId` - **Polymarket internal user UUID** (e.g., `c4e2a911-36ec-4453-8a39-1edb5e6b2969`)
- `platformId` - Twitter/X native Snowflake ID (e.g., `1994334744125874243`)
---
## Key Differences
| Aspect | UI Download | API Download |
|--------|-------------|--------------|
| **ID Type** | Twitter Snowflake ID | Polymarket CUID |
| **ID Column Name** | `Tweet ID` | `id` |
| **ID Example** | `1994149756910883262` | `cmiip5awe0000i004bhwnpcnb` |
| **ID Length** | 19 digits | 25 characters |
| **ID Format** | Numeric | Alphanumeric (CUID) |
| **User ID Type** | Twitter handle (string) | Polymarket UUID |
| **User Column** | `User` | `userId` |
| **User Example** | `elonmusk` | `c4e2a911-36ec-4453-8a39-1edb5e6b2969` |
| **Platform ID** | N/A (not included) | `platformId` (Twitter ID) |
---
## Why Are They Different?
### UI Download
- **Source:** Polymarket web interface export feature
- **Format:** User-friendly, shows Twitter-native IDs
- **Purpose:** Human-readable, easy to match with Twitter
- **Column:** Uses Twitter original Snowflake IDs directly
### API Download
- **Source:** Polymarket REST API endpoint
- **Format:** Database-native, shows internal IDs
- **Purpose:** Programmatic access, database referential integrity
- **Column:** Uses Polymarket internal CUIDs + includes Twitter ID separately
---
## Field Mapping Between UI and API
| UI CSV Column | API CSV Column | Description |
|---------------|----------------|-------------|
| `Tweet ID` | `platformId` | Twitter Snowflake ID (same value) |
| `User` | (not included) | Twitter handle (string) |
| (not included) | `id` | Polymarket internal tweet ID |
| (not included) | `userId` | Polymarket internal user UUID |
| `Content` | `content` | Tweet text (same) |
| `Posted At (EST)` | `createdAt` | Tweet timestamp (different format) |
| `Imported At (EST)` | `importedAt` | Import timestamp (different format) |
---
## Which ID Should You Use?
### For Matching with Twitter
✅ **Use `platformId` from API** or **`Tweet ID` from UI**
- Both contain the same Twitter Snowflake ID
- Can be used to construct tweet URLs: `https://twitter.com/user/status/{platformId}`
- Can be used to fetch tweet data from Twitter API
### For Polymarket Database Queries
✅ **Use `id` from API**
- Polymarket internal reference
- Needed for API calls to Polymarket
- Unique within Polymarket system
---
## Why Does Polymarket Use Different IDs?
### Database Design Best Practices
1. **Internal IDs (CUIDs):**
   - Prevents coupling to external platform IDs
   - Maintains data integrity if Twitter changes IDs
   - Enables Polymarket to track same tweet across platforms
   - Better for database indexing and relationships
2. **Platform IDs (Twitter Snowflakes):**
   - Preserved as reference to original source
   - Enables linking back to Twitter
   - Useful for data validation and deduplication
3. **User UUIDs:**
   - Internal user management
   - Same user might have multiple platform accounts
   - Privacy and security considerations
---
## Summary
**The IDs are different because:**
1. **UI Download** shows **Twitter-native IDs** for user-friendliness
2. **API Download** shows **Polymarket-internal IDs** for database integrity
3. **Both contain the Twitter ID**, just in different columns:
   - UI: `Tweet ID` column
   - API: `platformId` column
**To reconcile:**
- Match `Tweet ID` (UI) with `platformId` (API)
- Ignore the `id` and `userId` from API if you only care about Twitter data
- Use `id` and `userId` from API if you re building integration with Polymarket
---
**Conclusion:** The IDs are different because UI shows Twitter-native IDs while API shows Polymarket-internal IDs. Both formats contain the same Twitter Snowflake ID - just in different columns (`Tweet ID` vs `platformId`). Use `platformId` to match tweets between the two formats.
