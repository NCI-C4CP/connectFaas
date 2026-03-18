# SendGrid Email Deliverability Remediation Plan

## Context

Email deliverability is ~82% for the Connect for Cancer Prevention Study (~200K participants). SendGrid reports excessive bounces, drops, defers, and blocks. The codebase has several structural issues: no bounce/spam suppression (bounced addresses get re-emailed every cycle), no RFC 8058 List-Unsubscribe headers (mandatory since Feb 2024 for bulk senders), fire-and-forget email sending, no pre-send suppression checks, no retry logic, and no plaintext alternative in emails. The from address is `no-reply-myconnect@mail.nih.gov`. Auth magic-link email (Microsoft Graph) is out of scope.

---

## Phase 1: Critical Fixes (Immediate Deliverability Impact)

### 1A. Mail-Stream Classifier

Classify every participant email as **bulk** or **operational** at send time. This drives unsubscribe behavior, concurrency limits, and future subdomain separation.

**File**: `utils/notifications.js`

- At line 461, the code already identifies `newsletterCategories = ["newsletter", "eNewsletter", "anniversaryNewsletter"]`
- Add a `mailStream` variable: `const mailStream = newsletterCategories.includes(notificationSpec.category) ? "bulk" : "operational"`
- Pass `mailStream` into `custom_args` on every email (replaces `token`/`connect_id`)
- Use `mailStream` to determine unsubscribe behavior (1C) and concurrency limits (2A)

### 1B. Email Suppression System

**The #1 issue**: `processSendGridEvent()` in `utils/firestore.js:5049` records bounces/spam in the notification document but **never suppresses** the participant from future sends. Hard-bounced addresses get re-emailed every scheduled notification, compounding reputation damage. Compare to SMS: `processTwilioEvent()` at line 5103 already calls `updateSmsPermission()` on errors — email needs the same pattern.

**Approach**: Both a Firestore `emailAddressStatus` collection (fast lookups, rich schema) AND participant concept ID fields (dashboard visibility).

**`emailAddressStatus` schema** (doc keyed by normalized lowercase email):
```
normalizedEmail, status, reason, sourceEvent, lastEventAt, lastNotificationId,
suppressBulk, suppressOperational, manualOverride
```

**Suppression policy by event type**:
| Event | suppressBulk | suppressOperational | Action |
|-------|-------------|-------------------|--------|
| Hard bounce (`event.event === "bounce"`, `event.type === "bounce"`) | true | true | Suppress all |
| Invalid email (dropped with invalid reason) | true | true | Suppress all |
| Spam report (`event.event === "spamreport"`) | true | true | Suppress all |
| Unsubscribe | true | false | Suppress bulk only |
| Block (`event.event === "bounce"`, `event.type === "blocked"`) | false | false | Record + alert, no auto-suppress |
| Defer | false | false | Record + count only |

**Files to modify**:

1. **`utils/firestore.js`** — Add functions:
   - `addEmailSuppression(email, reason, notificationId, suppressBulk, suppressOperational)` — Write/update `emailAddressStatus` doc
   - `isEmailSuppressed(email, mailStream)` — Check suppression for the given stream
   - `getEmailSuppressions(emailArray, mailStream)` — Batch check using Firestore `in` queries (chunked to 30)
   - `updateEmailPermission(email, isPermitted, reason)` — Update participant document's `canWeEmail` concept ID field (modeled on `updateSmsPermission` at line 5437)

2. **`utils/firestore.js:5068-5078`** — Modify `processSendGridEvent()`:
   - Expand stored event data: `event`, `reason`, `response`, `status`, `type`, `attempt`, `sg_event_id`, `sg_message_id`, and event timestamp
   - After recording the event, apply suppression policy per table above
   - Call `addEmailSuppression()` and `updateEmailPermission()` as appropriate

3. **`utils/fieldToConceptIdMapping.js`** — Add concept IDs for `canWeEmail`, `emailSuppressionReason`, `emailSuppressionDate` (coordinate with data dictionary team)

### 1C. Pre-Send Suppression Check + Email Normalization

**Email normalization**: Before any validation or send, normalize all email addresses: `email.trim().toLowerCase()`. Apply this at:
- `utils/notifications.js:567` (before `validEmailFormat.test()`)
- `utils/notifications.js:1277` (instant notification)
- `utils/validation.js:600` (email validation API)
- Any participant email write paths

**Pre-send suppression gate** (after BigQuery candidate selection, before `sgMail.send`):

1. **`utils/notifications.js:549`** — In `handleNotificationSpec()`:
   - Normalize emails, collect all from current batch
   - Call `getEmailSuppressions(emailArray, mailStream)` to get suppressed set
   - Skip suppressed emails in the participant loop
   - Track `skippedBySuppression` count for logging

2. **`utils/notifications.js:1249`** — In `sendInstantNotification()`:
   - Normalize email, check `isEmailSuppressed(email, mailStream)` before sending

3. **`utils/bigquery.js`** — Once concept IDs are established, add WHERE clause to `getParticipantsForNotificationsBQ()` to filter suppressed participants at query level (optimization for 200K participants)

### 1D. List-Unsubscribe Headers (RFC 8058) — Bulk Mail Only

Google/Yahoo require RFC 8058 one-click unsubscribe for bulk senders (>5000/day). Current code only uses in-body `<% click here %>` via SendGrid subscription tracking.

**Key decision**: One-click unsubscribe applies to **bulk mail only** (newsletters). Operational study reminders do not use bulk unsubscribe.

**Files to modify**:

1. **`utils/notifications.js:656-670`** — In `handleNotificationSpec()` email batch construction:
   - If `mailStream === "bulk"`: add `asm: { group_id: parseInt(process.env.SG_ASM_GROUP_BULK) }` and `headers` with `List-Unsubscribe` + `List-Unsubscribe-Post`
   - If `mailStream === "operational"`: do NOT add ASM group or List-Unsubscribe headers
   - Keep existing `subscription_tracking` for in-body link on bulk only

2. **`utils/webhook.js:70-78`** — Add new route `api=email-unsubscribe`:
   - Accept POST requests (RFC 8058 one-click)
   - Look up notification by ID, extract email
   - Call `addEmailSuppression()` with `suppressBulk: true, suppressOperational: false`

3. **Verify account-level**: Check if SendGrid subscription tracking already adds these headers. If so, ensure it's only enabled for bulk, not operational.

### 1E. Fix Fire-and-Forget in `sendEmail()`

**File**: `utils/notifications.js:372-390`

- Change `sgMail.send(msg).then().catch()` to `await sgMail.send(msg)` with try/catch
- Propagate errors via `throw` so callers can handle them
- Fix caller in `utils/firestore.js:2511` to add `await`
- `utils/siteNotifications.js:58` already uses `await` — will now properly catch errors

### 1F. Plaintext Alternative

Spam filters penalize HTML-only emails. Currently no `text` property is set on any SendGrid payload.

**Files to modify**:

1. **`utils/notifications.js:656-670`** — Add `text` property to `emailBatch` generated from the HTML body (strip tags, preserve line breaks)
2. **`utils/notifications.js:1268-1293`** — Same for `sendInstantNotification()`
3. **`utils/notifications.js:374-382`** — Same for `sendEmail()`
4. **`utils/shared.js`** — Add `htmlToPlaintext(html)` utility that strips tags and converts `<br>`, `<p>`, `<li>` to newlines

### 1G. custom_args Sanitization

Remove PII-adjacent data from SendGrid's servers.

**File**: `utils/notifications.js:606-611` and `1279-1284`

Replace with:
```
custom_args: {
  notification_id: emailId,
  notification_spec_id: notificationSpec.id,
  notification_category: notificationSpec.category,
  mail_stream: mailStream,
  language: prefLang,
  gcloud_project: process.env.GCLOUD_PROJECT,
}
```
Remove `token` and `connect_id`. These are already stored in the Firestore notification record and looked up via `notification_id` in the webhook handler.

### 1H. Backfill Script

Before enabling the suppression gate, import existing SendGrid suppressions.

**New file**: `scripts/backfillEmailSuppressions.js`

- Export current SendGrid suppressions (bounces, invalid emails, spam reports, unsubscribes, blocks) from SendGrid API or CSV export
- For each, write to `emailAddressStatus` collection with appropriate `suppressBulk`/`suppressOperational` flags
- Log counts for verification

---

## Phase 2: Architecture Improvements

### 2A. Bounded Send Concurrency

**File**: `utils/notifications.js` — In `sendScheduledNotifications()`:
- At most 1 bulk spec in flight at a time
- At most 2 operational specs in flight at a time
- Configurable via `SG_SEND_MAX_BULK_CONCURRENCY` (default 1) and `SG_SEND_MAX_OPERATIONAL_CONCURRENCY` (default 2)

### 2B. Retry Logic with Backoff

**File**: `utils/notifications.js:672-684` — Replace `break` on error:
- Retry up to `SG_SEND_RETRY_MAX` (default 5) times for 429 and 5xx responses with exponential backoff + jitter
- On final failure, `continue` to next language instead of `break`
- Separate error handling for `sgMail.send()` vs `saveNotificationBatch()`

### 2C. Per-Spec Send Summaries

**File**: `utils/notifications.js` — After each spec completes:
- Log: accepted count, skipped-by-suppression count, retried count, submit-failed count, by language and mail stream
- Store summary in Firestore on the notification spec document for dashboard visibility

### 2D. Webhook Processing Performance

**File**: `utils/webhook.js:49-52` — Replace sequential `for...of` with:
- `Promise.allSettled()` with concurrency limit of 10
- Log failures without blocking webhook response

### 2E. Richer Webhook Event Storage

**File**: `utils/firestore.js:5071-5078` — Expand notification document updates:
- Store: `event`, `reason`, `response`, `status`, `type`, `attempt`, `sg_event_id`, `sg_message_id`, event timestamp
- This gives visibility into block/defer causes for debugging

### 2F. Reply-To Header

**File**: `utils/notifications.js` — Add `replyTo` to all three email construction sites (lines 374, 656, 1268):
```
replyTo: { email: process.env.SG_REPLY_TO_EMAIL || "ConnectCC@nih.gov", name: "Connect Support" }
```

---

## Phase 3: External / Non-Code Actions

1. **SendGrid Account Audit** (before rollout): Confirm whether SendGrid is already dropping suppressed recipients; inspect suppression lists; verify Event Webhook delivery; confirm mail settings; capture baseline screenshots/exports for dropped reasons
2. **Export current SendGrid suppressions**: bounces, invalid emails, spam reports, unsubscribes, blocks — feed into backfill script (1H)
3. **Verify SendGrid architecture**: shared vs dedicated IPs, IP warmup state, subusers, IP pools, link branding, reverse DNS. If shared IPs, plan move to dedicated. If dedicated IPs are cold, warm before large campaigns
4. **DNS Authentication Audit**: Verify SPF includes `include:sendgrid.net`, DKIM passes aligned for `mail.nih.gov`, DMARC at `p=quarantine` or `p=reject`. Verify with real delivered message raw headers, not just DNS records
5. **Move dev/stage off production sender domain**: Use a non-prod sender subdomain or sink/test domain. Block real participant sends from lower environments
6. **Google Postmaster Tools**: Register sending domain, monitor compliance dashboard
7. **Microsoft SNDS/JMRP**: Register dedicated IPs, monitor reputation
8. **Set operational thresholds**: Pause bulk sends if spam complaints reach 0.1%, or if block/defer rates spike materially week-over-week
9. **Subdomain separation**: Deferred — plan separately when DNS coordination is ready
10. **BIMI**: Government domains can display official seals — implement after DMARC reaches `p=reject`
11. **Template audit**: Review all live notification templates stored outside the repo; remove unnecessary sensitive content

---

## Key Files

| File | Changes |
|------|---------|
| `utils/firestore.js` | Suppression functions, modify `processSendGridEvent()` for richer event storage + suppression, fix `sendEmail` caller |
| `utils/notifications.js` | Mail-stream classifier, pre-send suppression check, email normalization, List-Unsubscribe (bulk only), fix `sendEmail()`, plaintext alternative, custom_args cleanup, bounded concurrency, retry logic, per-spec summaries, Reply-To |
| `utils/webhook.js` | Unsubscribe route, parallel event processing |
| `utils/shared.js` | Add `htmlToPlaintext()`, reuse `unsubscribeTextObj`, `delay`, `validEmailFormat` |
| `utils/fieldToConceptIdMapping.js` | New concept IDs for `canWeEmail`, `emailSuppressionReason`, `emailSuppressionDate` |
| `scripts/backfillEmailSuppressions.js` | New — import existing SendGrid suppressions |

## Existing Utilities to Reuse
- `delay()` from `utils/shared.js` — already imported in notifications.js
- `updateSmsPermission()` pattern from `utils/firestore.js:5437` — model email suppression after this
- `SmsBatchSender` retry pattern from `utils/notifications.js:80-297` — reference for email retry logic
- `validEmailFormat` from `utils/shared.js:2144` — already used for email validation
- `newsletterCategories` from `utils/notifications.js:461` — basis for mail-stream classifier

## New Config

| Variable | Default | Purpose |
|----------|---------|---------|
| `SG_ASM_GROUP_BULK` | (required for bulk) | SendGrid ASM group ID for bulk mail unsubscribe |
| `SG_SEND_MAX_BULK_CONCURRENCY` | 1 | Max bulk specs in-flight |
| `SG_SEND_MAX_OPERATIONAL_CONCURRENCY` | 2 | Max operational specs in-flight |
| `SG_SEND_RETRY_MAX` | 5 | Max retries on 429/5xx |
| `EMAIL_BATCH_DELAY_MS` | 1000 | Delay between batch sends |
| `SG_REPLY_TO_EMAIL` | ConnectCC@nih.gov | Reply-To address |

## Testing

Tests follow existing `require.cache` CJS mocking pattern in `test/unit/notifications.test.js`:

- **`test/unit/emailSuppression.test.js`**: `addEmailSuppression`, `isEmailSuppressed`, `getEmailSuppressions`, `updateEmailPermission`, suppression policy by event type
- **Extend `test/unit/notifications.test.js`**: Mail-stream classification, pre-send suppression filtering, email normalization, plaintext generation, retry logic, custom_args sanitization, per-spec summaries
- **Extend `test/unit/apiEndpoints.test.js`**: Unsubscribe webhook endpoint (POST/GET/missing ID)
- **Extend existing webhook tests**: `processSendGridEvent` calls suppression for hard bounces + spam, records rich event data, does NOT suppress on blocks/defers

## Verification

1. **Unit tests**: `npm test` — all new and existing tests pass
2. **Baseline validation**: Compare app send attempts vs SendGrid Email Activity + suppression exports to understand what SendGrid already filters
3. **Header verification**: Send sample bulk and operational emails to seeded Gmail/Outlook/Yahoo inboxes. Inspect raw headers: `List-Unsubscribe` present on bulk only, DKIM aligned, plaintext part present
4. **Backfill verification**: Imported suppressions prevent future sends before any new webhook events occur
5. **Deploy to stage**: Trigger scheduled notifications with test participants, verify suppressed emails are skipped, per-spec summaries log correctly
6. **Webhook testing**: Send test bounce/spam/block events to webhook endpoint, verify `emailAddressStatus` docs and participant docs updated correctly
7. **Post-deploy monitoring**: SendGrid dashboard bounce/drop rates over 2 weeks; Postmaster Tools compliance status; weekly reporting slice (top droppedReason, bounceReason, block/defer rates by domain, suppression growth, skipped counts)

## Success Criteria
- No repeated sends to hard-bounced or spam-reported addresses
- No bulk sends to unsubscribed addresses
- Every SendGrid notification tagged by mail stream and spec
- Block/defer causes visible in stored events and dashboards
- SendGrid-side filtering behavior explicitly documented from live evidence
- Non-prod no longer uses the production sender domain
