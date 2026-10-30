BEGIN;

ALTER TABLE "public"."channels" ADD COLUMN "autopush_channel_id" UUID;
UPDATE "public"."channels" SET "autopush_channel_id" = "channel_id" WHERE "autopush_channel_id" IS NULL;
ALTER TABLE "public"."channels" ALTER COLUMN "autopush_channel_id" SET NOT NULL;
CREATE UNIQUE INDEX "channels_autopush_channel_id_idx" ON "public"."channels" ("autopush_channel_id");

CREATE TABLE "public"."channel_fcm_tokens" (
  "channel_id" UUID NOT NULL REFERENCES "public"."channels" ("channel_id") ON DELETE CASCADE,
  "fcm_token" TEXT NOT NULL,
  "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  PRIMARY KEY ("channel_id", "fcm_token")
);

CREATE INDEX "channel_fcm_tokens_fcm_token_idx" ON "public"."channel_fcm_tokens" ("fcm_token");

INSERT INTO "public"."channel_fcm_tokens" ("channel_id", "fcm_token", "created_at", "updated_at")
SELECT "channel_id", "fcm_token", "created_at", "updated_at" FROM "public"."channels";

DROP INDEX IF EXISTS "public"."channels_fcm_token_idx";
ALTER TABLE "public"."channels" DROP COLUMN "fcm_token";

COMMIT;
