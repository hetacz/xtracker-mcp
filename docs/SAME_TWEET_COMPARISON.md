# ID Comparison - Same Tweet, Different Formats
**Tweet:** "RT @EricLDaugh: 🚨 JUST IN: Sec. Marco Rubio orders..."  
**Posted:** November 21, 2025  
**Twitter ID:** `1992015561900073427`
---
## UI Download Format
```csv
Tweet ID,User,Content,Posted At (EST),Posted Date,Posted Time,Imported At (EST),Imported Date,Imported Time
1992015561900073427,elonmusk,"RT @EricLDaugh...",11/21/2025, 6:41:34 PM,11/21/2025,6:41:34 PM,11/24/2025, 1:24:36 AM,11/24/2025,1:24:36 AM
```
### Fields:
- **Tweet ID:** `1992015561900073427` ← **Twitter native ID**
- **User:** `elonmusk` ← Twitter handle (string)
- **Posted At:** `11/21/2025, 6:41:34 PM` (EST format, human-readable)
- **Imported At:** `11/24/2025, 1:24:36 AM` (EST format)
---
## API Download Format
```csv
id,userId,platformId,content,createdAt,importedAt,metrics
cmicri0u8002ejr04zp78qtrm,c4e2a911-36ec-4453-8a39-1edb5e6b2969,1992015561900073427,"RT @EricLDaugh...",2025-11-21T23:41:34.000Z,2025-11-24T06:24:36.796Z,
```
### Fields:
- **id:** `cmicri0u8002ejr04zp78qtrm` ← **Polymarket internal ID (CUID)**
- **userId:** `c4e2a911-36ec-4453-8a39-1edb5e6b2969` ← Polymarket user UUID
- **platformId:** `1992015561900073427` ← **Twitter native ID (same as UI "Tweet ID")**
- **createdAt:** `2025-11-21T23:41:34.000Z` (UTC format, ISO 8601)
- **importedAt:** `2025-11-24T06:24:36.796Z` (UTC format)
---
## Direct Field Comparison
| Field | UI Value | API Value | Match? |
|-------|----------|-----------|--------|
| **Twitter ID** | `1992015561900073427` | `1992015561900073427` | ✅ **SAME** |
| **Column Name** | `Tweet ID` | `platformId` | Different name |
| **User Identifier** | `elonmusk` (handle) | `c4e2a911-36ec-4453-8a39-1edb5e6b2969` (UUID) | Different format |
| **Internal ID** | N/A | `cmicri0u8002ejr04zp78qtrm` (CUID) | API only |
| **Content** | "RT @EricLDaugh..." | "RT @EricLDaugh..." | ✅ **SAME** |
| **Posted Time** | `11/21/2025, 6:41:34 PM` (EST) | `2025-11-21T23:41:34.000Z` (UTC) | ✅ **SAME** (6:41 PM EST = 11:41 PM UTC) |
| **Imported Time** | `11/24/2025, 1:24:36 AM` (EST) | `2025-11-24T06:24:36.796Z` (UTC) | ✅ **SAME** (1:24 AM EST = 6:24 AM UTC) |
---
## Key Insight
**Both files contain the same Twitter ID - just in different columns!**
**To match them:** Use `Tweet ID` (UI) = `platformId` (API)
**Summary:** The IDs appear different because UI shows Twitter-native IDs while API shows Polymarket-internal IDs. However, the Twitter ID is preserved in both formats.
