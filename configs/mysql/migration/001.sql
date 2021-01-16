-- Add ":new" property to all existing documents.
-- This property is now used to track for a specific new document
INSERT INTO property (name, document, revision) SELECT ":new", id, 1 FROM document
ON DUPLICATE KEY UPDATE name = VALUES(name);

-- Add columen created to track whether the document actually was created.
ALTER TABLE document
ADD COLUMN created TINYINT(1) NOT NULL AFTER name;

-- All docs that are currently in the database exist.
UPDATE document SET created = 1 WHERE id > 0;