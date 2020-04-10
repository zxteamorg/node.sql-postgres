CREATE TABLE "subscriber"
(
	"id" SERIAL NOT NULL PRIMARY KEY,
	"subscriber_uuid" UUID NOT NULL,
	"topic_id" INT REFERENCES "topic"("id") NOT NULL,
	"utc_create_date" TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
	"utc_delete_date" TIMESTAMP WITHOUT TIME ZONE NULL,
	CONSTRAINT "uq__subscriber__subscriber_uuid" UNIQUE ("subscriber_uuid")
);

CREATE VIEW "topic_view" AS
	SELECT * FROM "topic";

CREATE VIEW "subscriber_view" AS
	SELECT * FROM "subscriber";
