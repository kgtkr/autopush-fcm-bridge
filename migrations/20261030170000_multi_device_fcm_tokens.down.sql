BEGIN;

ALTER TABLE "public"."channels" ADD COLUMN "fcm_token" TEXT;

UPDATE "public"."channels" c
SET "fcm_token" = COALESCE(
  (
    SELECT cft."fcm_token"
    FROM "public"."channel_fcm_tokens" cft
    WHERE cft."channel_id" = c."channel_id"
    ORDER BY cft."updated_at" DESC
    LIMIT 1
  ),
  ''
);

ALTER TABLE "public"."channels" ALTER COLUMN "fcm_token" SET NOT NULL;
CREATE INDEX "channels_fcm_token_idx" ON "public"."channels" ("fcm_token");

DROP TABLE "public"."channel_fcm_tokens";
DROP INDEX IF EXISTS "public"."channels_autopush_channel_id_idx";
ALTER TABLE "public"."channels" DROP COLUMN "autopush_channel_id";

COMMIT;
