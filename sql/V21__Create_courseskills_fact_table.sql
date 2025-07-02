-- This script creates two join tables to manage the many-to-many relationships
-- between courses and skills, and between courses and taxonomy.
-- This is intended to be used as a single Flyway migration.

-- Table to link courses and skills
CREATE TABLE dbo.course_skills (
    course_id UNIQUEIDENTIFIER NOT NULL,
    skill_id UNIQUEIDENTIFIER NOT NULL,

    -- Composite primary key to ensure each course-skill pair is unique
    CONSTRAINT pk_course_skills PRIMARY KEY (course_id, skill_id),

    -- Foreign key to the courses table
    CONSTRAINT fk_course_skills_course FOREIGN KEY (course_id) 
        REFERENCES dbo.courses (course_id)
        ON DELETE CASCADE, -- If a course is deleted, its skill relationships are also deleted

    -- Foreign key to the skills table
    CONSTRAINT fk_course_skills_skill FOREIGN KEY (skill_id) 
        REFERENCES dbo.skills (skill_id)
        ON DELETE CASCADE -- If a skill is deleted, its course relationships are also deleted
);