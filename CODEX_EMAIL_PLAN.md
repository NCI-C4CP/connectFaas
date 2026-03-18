# SendGrid Deliverability Remediation Plan

## Summary
- Scope: SendGrid-backed participant notifications only.
- Out of scope: Microsoft Graph magic-link auth mail.
- Primary goal: improve deliverability by preventing repeat sends to bad addresses, separating bulk vs operational behavior, capturing actionable SendGrid telemetry, and validating live account-side behavior.
- Working default: one-click unsubscribe applies to bulk/newsletter mail only, not operational study reminders.
- Main implementation areas: `utils/notifications.js`, `utils/firestore.js`, and `utils/bigquery.js`.

## Phase 1: High-Impact Code Fixes
- Add a mail-stream classifier in the SendGrid participant send path:
  `newsletter`, `eNewsletter`, and `anniversaryNewsletter` => `bulk`;
  all other participant SendGrid notifications => `operational`.
- Remove sensitive SendGrid metadata from `custom_args`.
  Keep only non-sensitive identifiers such as `notification_id`, `notification_spec_id`, `notification_category`, `mail_stream`, `language`, and `gcloud_project`.
  Remove `token` and `connect_id`.
- Add a Firestore suppression store keyed by normalized email address.
  Use it as the source of truth for pre-send suppression decisions.
- Normalize email addresses at write/send time.
  Trim whitespace, lowercase, reject empty strings before regex validation.
- Add pre-send suppression filtering in scheduled and instant notification paths.
  Suppressed addresses must be skipped before SendGrid submission.
- Expand SendGrid webhook processing to persist rich event data:
  `event`, `reason`, `response`, `status`, `type`, `attempt`, `sg_event_id`, `sg_message_id`, and timestamp.
- Update suppression state from webhook events with this policy:
  hard bounce / invalid / spam report => suppress bulk and operational;
  unsubscribe => suppress bulk only;
  block => record and alert, no automatic suppression;
  defer => record and count only.
- Fix `sendEmail()` so it actually awaits SendGrid submission and propagates failures.
- Add explicit SendGrid retry/backoff for `429` and `5xx` responses.
  Cap retries and log retry counts per spec.
- Add `replyTo` to participant-facing SendGrid mail, using an operational mailbox such as `ConnectCC@nih.gov`, subject to operations confirmation.
- Add a plaintext alternative for every participant SendGrid email.

## Phase 1 Validation Steps
- Verify whether SendGrid is already filtering bad sends on its side:
  compare app-side send attempts against SendGrid suppression exports, Email Activity, and `dropped` webhook events with reasons.
- Verify whether one-click unsubscribe is already present on live participant mail:
  inspect raw delivered headers for `List-Unsubscribe` and `List-Unsubscribe-Post: List-Unsubscribe=One-Click`.
- Treat raw message headers as the source of truth, not inbox UI buttons.
- Inventory every participant-facing SendGrid path and classify it as bulk or operational.
  Any participant-facing mail still using the generic helper must be migrated or explicitly excluded.

## Phase 2: Throughput And Reliability Improvements
- Replace unbounded scheduled-send concurrency with bounded concurrency.
  Allow limited parallelism for operational specs and stricter serialization for bulk specs.
- Add per-spec send summaries:
  accepted, skipped-by-suppression, retried, submit-failed, by language and stream.
- Improve webhook processing so it handles event batches efficiently without serial bottlenecks, while preserving observability and failure logging.
- Add a one-time suppression backfill process from SendGrid exports before pre-send suppression is enabled in production.
- Extend downstream notification export/reporting so richer SendGrid event fields and suppression data are available for analysis.
- Defer BigQuery-level suppression filtering until the Firestore suppression model is stable and the reporting/export path is defined.

## Outside The Codebase
- Audit the live SendGrid account before rollout:
  suppression lists, Email Activity reasons, Event Webhook configuration, mail settings, tracking settings, shared vs dedicated IPs, IP warming state, subusers, IP pools, link branding, and reverse DNS.
- Export current SendGrid suppressions:
  bounces, invalid emails, spam reports, unsubscribes, and blocks if available.
  Use that export to seed the suppression store.
- Keep bulk and operational traffic isolated operationally where SendGrid account structure allows it.
  Prefer separate subuser/IP pool later even if code ships first.
- Verify live authentication from real delivered samples.
  DKIM alignment for `mail.nih.gov` is the critical live check.
- Move dev/stage off `no-reply-myconnect@mail.nih.gov`.
  Use a non-prod sender subdomain or a sink/test domain and prevent real participant sends from lower environments.
- Register the sending domain/subdomains with Google Postmaster Tools and Microsoft SNDS/JMRP.
- Define campaign guardrails:
  pause bulk sends if spam complaint rate reaches `0.1%`, or if block/defer rates rise materially.
- Audit externally managed notification templates and remove unnecessary sensitive content.

## Interfaces And Data
- No public API changes.
- New internal config expected:
  `SG_ASM_GROUP_BULK`, `SG_SEND_MAX_OPERATIONAL_CONCURRENCY`, `SG_SEND_MAX_BULK_CONCURRENCY`, `SG_SEND_RETRY_MAX`, and `SG_REPLY_TO_EMAIL`.
- New internal data store:
  Firestore suppression collection keyed by normalized email.
- Existing `notifications` records gain richer SendGrid event fields for debugging and analytics.

## Test Plan
- Unit tests for:
  mail-stream classification, email normalization, `custom_args` sanitization, suppression decisions, webhook event mapping, retry behavior, and true async error propagation from `sendEmail()`.
- Integration-style tests for:
  scheduled sends skipping suppressed addresses, instant sends rejecting suppressed addresses, bulk unsubscribe suppressing only bulk, and hard-bounce/spam-report events suppressing future mail.
- Manual seeded-inbox verification for Gmail/Outlook/Yahoo:
  bulk mail has one-click unsubscribe headers;
  operational mail does not;
  headers show valid authentication;
  webhook events persist expected reason/response fields.
- Backfill verification:
  imported suppressions prevent future sends before new webhook events arrive.
- Success criteria:
  no repeated sends to hard-bounced or spam-reported addresses;
  no bulk sends to unsubscribed addresses;
  SendGrid-side filtering behavior documented from live evidence;
  block/defer causes visible in stored data and reporting;
  non-prod no longer uses the production sender domain.

## Assumptions
- Auth magic-link mail remains unchanged and is ignored for this work.
- Bulk-only unsubscribe is the selected default.
- The suppression store is the source of truth for send gating.
- BigQuery remains the candidate source, not the final eligibility gate, in the first implementation pass.
- Raw headers are authoritative for one-click unsubscribe verification.
