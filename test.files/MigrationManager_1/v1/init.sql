CREATE TABLE "topic"
(
	"id" SERIAL NOT NULL PRIMARY KEY,
	"name" VARCHAR(256) NOT NULL,
	"description" VARCHAR(1028) NOT NULL,
	"media_type" VARCHAR(1028) NOT NULL,
	"topic_security" VARCHAR(1028) NOT NULL,
	"publisher_security" VARCHAR(1028) NOT NULL,
	"subscriber_security" VARCHAR(1028) NOT NULL,
	"utc_create_date" TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
	"utc_delete_date" TIMESTAMP WITHOUT TIME ZONE NULL,

	CONSTRAINT "uq_topic_name" UNIQUE ("name")
);
