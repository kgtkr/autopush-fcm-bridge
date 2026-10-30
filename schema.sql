CREATE TABLE "public"."autopush_sessions" (
  "uaid" VARCHAR(128) PRIMARY KEY,
  "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  "last_connected_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE "public"."channels" (
  "channel_id" UUID PRIMARY KEY,
  "autopush_channel_id" UUID NOT NULL,
  "uaid" VARCHAR(128) NOT NULL REFERENCES "public"."autopush_sessions" ("uaid") ON DELETE CASCADE,
  "app_server_key" VARCHAR(255) NOT NULL,
  "push_endpoint" TEXT NOT NULL,
  "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX "channels_autopush_channel_id_idx" ON "public"."channels" ("autopush_channel_id");
CREATE INDEX "channels_uaid_idx" ON "public"."channels" ("uaid");

CREATE TABLE "public"."channel_fcm_tokens" (
  "channel_id" UUID NOT NULL REFERENCES "public"."channels" ("channel_id") ON DELETE CASCADE,
  "fcm_token" TEXT NOT NULL,
  "created_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  PRIMARY KEY ("channel_id", "fcm_token")
);

CREATE INDEX "channel_fcm_tokens_fcm_token_idx" ON "public"."channel_fcm_tokens" ("fcm_token");
