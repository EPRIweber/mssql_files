-- Adds hydrogen-classification columns to dbo.course_taxonomy01
ALTER TABLE dbo.course_taxonomy01
ADD
    hydrogen_related     BIT           NOT NULL DEFAULT 0,   -- 1 = hydrogen-related
    classification_note  NVARCHAR(400) NULL;                 -- rationale text
