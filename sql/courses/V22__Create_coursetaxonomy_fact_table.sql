-- Table to link courses and taxonomy
CREATE TABLE dbo.course_taxonomy (
    course_id UNIQUEIDENTIFIER NOT NULL,
    taxonomy_id UNIQUEIDENTIFIER NOT NULL,

    -- Composite primary key to ensure each course-taxonomy pair is unique
    CONSTRAINT pk_course_taxonomy PRIMARY KEY (course_id, taxonomy_id),

    -- Foreign key to the courses table
    CONSTRAINT fk_course_taxonomy_course FOREIGN KEY (course_id) 
        REFERENCES dbo.courses (course_id)
        ON DELETE CASCADE, -- If a course is deleted, its taxonomy relationships are also deleted

    -- Foreign key to the taxonomy table
    CONSTRAINT fk_course_taxonomy_taxonomy FOREIGN KEY (taxonomy_id) 
        REFERENCES dbo.taxonomy (taxonomy_id)
        ON DELETE CASCADE -- If a taxonomy item is deleted, its course relationships are also deleted
);