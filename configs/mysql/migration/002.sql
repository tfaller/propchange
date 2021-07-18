BEGIN;

-- Add column docs to track which docs a listener listens to.
ALTER TABLE listener
ADD COLUMN docs json NOT NULL AFTER name;

-- Convert old listeners docs to new storage format.
UPDATE listener l SET docs = 
(
    SELECT JSON_OBJECTAGG(CONVERT(d.name USING utf8mb4), TRUE)
    FROM listener_document ld
    JOIN document d ON d.id = ld.document WHERE ld.listener = l.id
    GROUP BY ld.listener
);

-- Drop now unused table listener_document.
DROP TABLE listener_document;

-- insert pre-defined doc which is used if something does not exist (anymore).
INSERT INTO document (id, name, created) VALUES (-1, ':changed', 1);
INSERT INTO property (id, name, document, revision) VALUES (-1, ':changed', -1, 9223372036854775807);

COMMIT;