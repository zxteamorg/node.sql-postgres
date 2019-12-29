INSERT INTO "subscriber" ("subscriber_uuid", "topic_id") VALUES ('3048bc7b-6f07-4f2a-9a7b-9ce108d1b197', (SELECT "id" FROM "topic" WHERE "name" = 'market1'));
