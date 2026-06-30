-- ============================================================================
-- Catalog Notification System — initial schema
-- PostgreSQL 16, wal_level=logical (set in postgresql.conf / docker command)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Core domain tables
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS accounts (
                                        account_id      VARCHAR(64) PRIMARY KEY,
    name            VARCHAR(256) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    );

CREATE TABLE IF NOT EXISTS pos_channels (
                                            channel_id      VARCHAR(64) NOT NULL,
    account_id      VARCHAR(64) NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    name            VARCHAR(256) NOT NULL,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, channel_id)
    );

CREATE TABLE IF NOT EXISTS service_catalogs (
                                                id              BIGSERIAL PRIMARY KEY,
                                                account_id      VARCHAR(64) NOT NULL,
    channel_id      VARCHAR(64) NOT NULL,
    catalog_version BIGINT NOT NULL DEFAULT 1,
    catalog_payload JSONB NOT NULL,
    change_type     VARCHAR(32) NOT NULL DEFAULT 'Update', -- 'Update' | 'Critical'
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (account_id, channel_id) REFERENCES pos_channels(account_id, channel_id) ON DELETE CASCADE,
    UNIQUE (account_id, channel_id)
    );

CREATE INDEX IF NOT EXISTS ix_service_catalogs_account_channel
    ON service_catalogs (account_id, channel_id);

-- ----------------------------------------------------------------------------
-- Transactional outbox (the "safe path")
-- Written in the SAME transaction as the service_catalogs UPDATE.
-- OutboxRelayService polls WHERE processed_at IS NULL every 500ms.
-- id is used verbatim as the NATS MsgId for deduplication.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS outbox_messages (
                                               id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id      VARCHAR(64) NOT NULL,
    channel_id      VARCHAR(64) NOT NULL,
    subject         VARCHAR(256) NOT NULL,       -- e.g. catalog.acc123.pos001 or catalog.critical
    change_type     VARCHAR(32) NOT NULL,        -- 'Update' | 'Critical'
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at    TIMESTAMPTZ NULL
    );

-- Outbox relay polls this index hot-path
CREATE INDEX IF NOT EXISTS ix_outbox_unprocessed
    ON outbox_messages (created_at)
    WHERE processed_at IS NULL;





CREATE OR REPLACE FUNCTION fn_notify_catalog_changes() RETURNS TRIGGER AS $$
DECLARE
v_outbox_id UUID := gen_random_uuid();
    json_payload JSON;
    v_subject TEXT;
BEGIN
    v_subject := CASE WHEN NEW.change_type = 'Critical'
                 THEN 'catalog.critical'
                 ELSE 'catalog.' || NEW.account_id || '.' || NEW.channel_id END;

INSERT INTO outbox_messages (id, account_id, channel_id, subject, change_type, payload, created_at)
VALUES (v_outbox_id, NEW.account_id, NEW.channel_id, v_subject, NEW.change_type, NEW.catalog_payload, now());

json_payload := json_build_object(
        'outboxMessageId', v_outbox_id,
        'accountId',       NEW.account_id,
        'channelId',       NEW.channel_id,
        'version',         NEW.catalog_version,
        'changeType',      NEW.change_type,
        'updatedAt',       NEW.updated_at
    );

    PERFORM pg_notify('catalog_changes', json_payload::text);
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_notify_catalog_changes ON service_catalogs;

CREATE TRIGGER trg_notify_catalog_changes
    AFTER INSERT OR UPDATE ON service_catalogs
                        FOR EACH ROW
                        EXECUTE FUNCTION fn_notify_catalog_changes();








