-- =====================================================================
-- Test script: verifies trg_notify_catalog_changes / fn_notify_catalog_changes
-- against the fintech seed data (acc-fintech-01 / wire-transfer, etc).
--
-- Run interactively in two psql sessions to see pg_notify end-to-end:
--   Session A: LISTEN catalog_changes; (leave open)
--   Session B: run the UPDATE/INSERT statements below
-- =====================================================================

-- ---------------------------------------------------------------------
-- 0. Sanity check: seed data is present
-- ---------------------------------------------------------------------
SELECT account_id, channel_id, catalog_version, change_type, updated_at
FROM service_catalogs
ORDER BY account_id, channel_id;

-- ---------------------------------------------------------------------
-- 1. (Session A) Start listening BEFORE running the update in Session B
-- ---------------------------------------------------------------------
-- LISTEN catalog_changes;

-- ---------------------------------------------------------------------
-- 2. Baseline counts, captured before the test write
-- ---------------------------------------------------------------------
SELECT count(*) AS outbox_rows_before FROM outbox_messages;

-- ---------------------------------------------------------------------
-- 3. Trigger an UPDATE directly against service_catalogs — i.e. NOT
--    through the API. This is exactly the path that used to skip the
--    outbox before the trigger was changed to own row creation.
-- ---------------------------------------------------------------------
UPDATE service_catalogs
SET catalog_version = catalog_version + 1,
    catalog_payload = catalog_payload || '{"note": "fx spread adjusted"}'::jsonb,
    change_type     = 'Update',
    updated_at      = now()
WHERE account_id = 'acc-fintech-01'
  AND channel_id  = 'fx-conversion';

-- Expect (Session A): a NOTIFY on catalog_changes with a JSON payload
-- containing outboxMessageId, accountId=acc-fintech-01, channelId=fx-conversion.

-- ---------------------------------------------------------------------
-- 4. Confirm exactly one new outbox row was created by the trigger,
--    with processed_at still NULL (relay/listener hasn't published yet).
-- ---------------------------------------------------------------------
SELECT id, account_id, channel_id, subject, change_type, processed_at, created_at
FROM outbox_messages
WHERE account_id = 'acc-fintech-01' AND channel_id = 'fx-conversion'
ORDER BY created_at DESC
LIMIT 1;

SELECT count(*) AS outbox_rows_after FROM outbox_messages;
-- outbox_rows_after should be outbox_rows_before + 1

-- ---------------------------------------------------------------------
-- 5. Verify subject routing: Critical changes go to 'catalog.critical',
--    everything else to 'catalog.{accountId}.{channelId}'
-- ---------------------------------------------------------------------
UPDATE service_catalogs
SET catalog_version = catalog_version + 1,
    change_type     = 'Critical',
    updated_at      = now()
WHERE account_id = 'acc-fintech-01'
  AND channel_id  = 'wire-transfer';

SELECT subject, change_type
FROM outbox_messages
WHERE account_id = 'acc-fintech-01' AND channel_id = 'wire-transfer'
ORDER BY created_at DESC
LIMIT 1;
-- Expect subject = 'catalog.critical'

-- ---------------------------------------------------------------------
-- 6. Simulate ListenNotifyService's payload lookup (what
--    TryGetOutboxPayloadAsync does) — fetch by the outbox id you saw
--    in the NOTIFY payload from Session A.
-- ---------------------------------------------------------------------
-- SELECT payload::text FROM outbox_messages WHERE id = '<outboxMessageId from NOTIFY>';

-- ---------------------------------------------------------------------
-- 7. Simulate the ack (what MarkProcessedAsync does) and confirm it's
--    idempotent — second run should affect 0 rows since processed_at
--    is no longer NULL.
-- ---------------------------------------------------------------------
UPDATE outbox_messages
SET processed_at = now()
WHERE account_id = 'acc-fintech-01' AND channel_id = 'wire-transfer'
  AND processed_at IS NULL
RETURNING id, processed_at;

UPDATE outbox_messages
SET processed_at = now()
WHERE account_id = 'acc-fintech-01' AND channel_id = 'wire-transfer'
  AND processed_at IS NULL;
-- Expect: UPDATE 0 (already processed — proves OutboxRelayService and
-- ListenNotifyService can race here safely without double-marking)

-- ---------------------------------------------------------------------
-- 8. What OutboxRelayService's poll query sees right now (should be
--    empty if steps 3-7 ran in order and everything got marked processed)
-- ---------------------------------------------------------------------
SELECT id, account_id, channel_id, subject, created_at
FROM outbox_messages
WHERE processed_at IS NULL
ORDER BY created_at;

-- ---------------------------------------------------------------------
-- 9. Simulate a "missed" notify: insert an outbox row directly and
--    leave it unprocessed, mimicking what OutboxRelayService should
--    pick up on its next poll.
-- ---------------------------------------------------------------------
INSERT INTO outbox_messages (account_id, channel_id, subject, change_type, payload, created_at)
VALUES (
    'acc-fintech-02', 'personal-loans', 'catalog.acc-fintech-02.personal-loans',
    'Update', '{"products": [{"sku": "LOAN-PERSONAL-3YR", "aprPct": 9.49}]}'::jsonb, now()
);

SELECT id, account_id, channel_id, processed_at
FROM outbox_messages
WHERE processed_at IS NULL;
-- This row should now show up — confirms the relay's hot-path index
-- (ix_outbox_unprocessed) would find it.

-- ---------------------------------------------------------------------
-- 10. Cleanup (optional) — remove rows created by this test script only
-- ---------------------------------------------------------------------
-- DELETE FROM outbox_messages
-- WHERE account_id IN ('acc-fintech-01', 'acc-fintech-02')
--   AND created_at >= now() - interval '10 minutes';